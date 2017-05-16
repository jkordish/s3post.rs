#![cfg_attr(feature="clippy", plugin(clippy))]
#![feature(plugin)]

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate flate2;
extern crate rusoto;
extern crate crossbeam;
extern crate chrono;
extern crate syslog;
extern crate walkdir;
extern crate ifaces;

use std::io;
use std::io::prelude::*;
use std::fs::{File, remove_file, create_dir_all};
use std::path::Path;
use std::env;
use std::str::FromStr;
use rusoto::{AutoRefreshingProvider, default_tls_client, DefaultCredentialsProvider, Region};
use rusoto::sts::{StsClient, StsAssumeRoleSessionCredentialsProvider};
use rusoto::s3::{S3Client, PutObjectRequest, Metadata};
use flate2::Compression;
use flate2::write::GzEncoder;
use chrono::prelude::Local;
use chrono::{Datelike, Timelike, Duration};
use crossbeam::scope;
use syslog::{Facility, Severity};
use walkdir::WalkDir;

#[derive(Serialize, Deserialize, Clone)]
struct ConfigFile {
    cachedir: String,
    role_arn: String,
    region: String,
    bucket: String,
    prefix: String,
}

trait Config {
    fn new(&self) -> ConfigFile;
}

impl Config for ConfigFile {
    fn new(&self) -> ConfigFile {
        ConfigFile {
            cachedir: self.cachedir.clone(),
            role_arn: self.role_arn.clone(),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

const MAX_LINES: usize = 50000;
const MAX_BYTES: usize = 10485760;
const MAX_TIMEOUT: i64 = 10;
const INTERFACE: &str = "eth0";

fn main() {

    let args: Vec<_> = env::args().collect();
    if args.len() < 1 {
        println!("s3post <config.json>");
    }

    let file_open = File::open(&args[1]).expect("could not open file");
    let config: ConfigFile = serde_json::from_reader(file_open).expect("config has invalid json");

    println!("MAX_LINES:\t{}\nMAX_BYTES:\t{}\nMAX_TIMEOUT:\t{}",
             MAX_LINES,
             MAX_BYTES,
             MAX_TIMEOUT);

    // attempt to resend any logs that we might have not successfully sent
    resend_logs(&config.clone());

    let reader = io::stdin();
    let mut buffer = reader.lock();
    let mut data = vec![0];
    let mut time = Local::now();
    let mut timeout = time + Duration::seconds(MAX_TIMEOUT);

    loop {
        match buffer.fill_buf() {
            Ok(bytes) => {
                // add the current bytes to our data vector
                data.extend(bytes);
                // evaulate if data and time meet of processing conditions
                if data.lines().count() >= MAX_LINES || data.len() >= MAX_BYTES ||
                   timeout <= time && !data.is_empty() {
                    // send the data to the compress function
                    compress(data.as_slice(), time, &config.clone());
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else {
                    // update the time
                    time = Local::now();
                    // attempt to resend any logs that we might have not successfully sent
                    resend_logs(&config.clone());
                }
            }
            Err(err) => panic!(err),
        }

        // consume the data from the buffer so we don't reprocess it
        buffer.consume(data.len());
    }
}

fn compress(bytes: &[u8], timestamp: chrono::DateTime<chrono::Local>, config: &ConfigFile) {

    // our compression routine will be sent to another thread
    scope(|scope| {
        scope.spawn(move || {

            // use our environment variable of hostname. will be essentially the CBID
            let hostname = match env::var("HOSTNAME") {
                Ok(hostname) => hostname,
                Err(err) => panic!("Unable to get hostname. {}", err),
            };

            // generate our local path
            let path = format!("{}/{}/{}/{}/{}/",
                               timestamp.year(),
                               timestamp.month(),
                               timestamp.day(),
                               timestamp.hour(),
                               timestamp.minute());

            // setting the format for how we write out the file.
            let file = format!("{}.{}-{}.raw.gz",
                               timestamp.second(),
                               timestamp.timestamp_subsec_millis(),
                               &hostname);

            // append the cachedir so we write to the correct place
            let fullpath = format!("{}/{}", &config.cachedir, &path);

            // create the directory structure
            let _ = create_dir_all(&fullpath);

            // create the file in the full path
            let mut output = File::create(format!("{}{}", &fullpath, &file)).unwrap();

            // create a gzip encoder
            let mut encoder = GzEncoder::new(Vec::new(), Compression::Default);

            // encode the retrieved bytes
            encoder.write_all(bytes).unwrap();
            let log = encoder.finish().unwrap();

            // write out the encoded data to our file
            output.write_all(&log).unwrap();

            // call write_s3 to send the gzip'd file to s3
            write_s3(&config.clone(), &file, &path, &log)
        });
    });
}

fn write_s3(config: &ConfigFile, file: &str, path: &str, log: &[u8]) {

    // move to new thread
    scope(|scope| {
        scope.spawn(move || {

            // need the ipaddr of our interface. will be part of the metadata
            let mut address = String::new();
            for iface in ifaces::Interface::get_all().unwrap() {
                if iface.name == INTERFACE && iface.kind == ifaces::Kind::Ipv4 {
                    address = format!("{}", iface.addr.unwrap());
                    address = address.replace(":0", "");
                } else if iface.name == "en0" && iface.kind == ifaces::Kind::Ipv4 {
                    address = format!("{}", iface.addr.unwrap());
                    address = address.replace(":0", "");
                } else {
                    panic!("Unable to find usable interface. Aborting!");
                }
            }

            // use our environment variable of hostname. will be essentially the CBID
            let hostname = match env::var("HOSTNAME") {
                Ok(hostname) => hostname,
                Err(err) => panic!("Unable to get hostname. {}", err),
            };

            let path = format!("{}/{}/{}", &config.prefix, &path, &file);

            // set up our credentials provider for aws
            let provider = match DefaultCredentialsProvider::new() {
                Ok(provider) => provider,
                Err(err) => panic!("Unable to load credentials. {}", err),
            };


            // initiate our sts client
            let sts_client = StsClient::new(default_tls_client().unwrap(),
                                            provider,
                                            Region::from_str(&config.region).unwrap());
            // generate a sts provider
            let sts_provider = StsAssumeRoleSessionCredentialsProvider::new(sts_client,
                                                                            config.role_arn
                                                                                .clone(),
                                                                            "s3post".to_owned(),
                                                                            None,
                                                                            None,
                                                                            None,
                                                                            None);
            // allow our STS to auto-refresh
            let auto_sts_provider = AutoRefreshingProvider::with_refcell(sts_provider);

            // create our s3 client initialization
            let s3 = S3Client::new(default_tls_client().unwrap(),
                                   auto_sts_provider.unwrap(),
                                   Region::from_str(&config.region).unwrap());

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            contents.extend(log);


            // generate our metadata which we add to the s3 upload
            let mut metadata = Metadata::new();
            metadata.insert("cbid".to_string(), hostname.to_string());
            metadata.insert("ip".to_string(), address.to_string());
            metadata.insert("uncompressed_bytes".to_string(), contents.len().to_string());
            metadata.insert("lines".to_string(), contents.lines().count().to_string());

            // if we can read the contents to the buffer we will attempt to send the log to s3
            // need to build our request
            let req = PutObjectRequest {
                bucket: config.bucket.clone(),
                key: path.to_owned(),
                body: Some(contents),
                metadata: Some(metadata),
                ..Default::default()
            };

            match s3.put_object(&req) {
                // we were successful!
                Ok(_) => {
                    // Send some notifications to SYSLOG
                    match syslog::unix(Facility::LOG_SYSLOG) {
                        Err(e) => println!("impossible to connect to syslog: {:?}", e),
                        Ok(writer) => {
                            let success = format!("Successfully wrote {}/{}", &req.bucket, &path);
                            let _ = writer.send_3164(Severity::LOG_ALERT, &success).is_ok();
                        }
                    }
                    // only remove the file if we are successful
                    let localpath = format!("{}/{}", &config.cachedir, &path);
                    if remove_file(&localpath).is_ok() {
                        // Send some notifications to SYSLOG
                        match syslog::unix(Facility::LOG_SYSLOG) {
                            Err(e) => println!("impossible to connect to syslog: {:?}", e),
                            Ok(writer) => {
                                let success = format!("Removed file {}", &localpath);
                                let _ = writer.send_3164(Severity::LOG_ALERT, &success).is_ok();
                            }
                        }
                    }
                }
                Err(e) => println!("Could not write file {} {}", &file, e),
            }
        });
    });
}

fn resend_logs(config: &ConfigFile) {

    // create iterator over the directory
    for entry in WalkDir::new(&config.cachedir).into_iter().filter_map(|e| e.ok()) {

        // filter out gzip'd file suffixes
        if entry.file_name()
               .to_str()
               .map(|s| s.ends_with(".gz"))
               .unwrap() {

            // get just the file name
            let filename = entry.file_name().to_str().unwrap();

            // need to return only the parent and strip off the cachedir prefix
            // this then is the path write_s3 is expecting as it is the path within s3
            let path =
                Path::new(entry.path().parent().unwrap()).strip_prefix(&config.cachedir).unwrap();
            let path = Path::new(path).to_str().unwrap();

            // build out the complete path for reading
            let filepath = format!("{}/{}/{}", &config.cachedir, &path, &filename);
            // open the file for reading
            let mut file = File::open(&filepath).expect("Unable to read file");

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            let _ = file.read_to_end(&mut contents);

            // Send some notifications to SYSLOG
            match syslog::unix(Facility::LOG_SYSLOG) {
                Err(e) => println!("impossible to connect to syslog: {:?}", e),
                Ok(writer) => {
                    let success = format!("Found unsent log {}/{}", &path, &filename);
                    let _ = writer.send_3164(Severity::LOG_ALERT, &success).is_ok();
                }
            }
            // pass the unset logs to s3
            write_s3(&config.clone(), filename, path, &contents)

        }
    }
}
