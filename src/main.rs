#![cfg_attr(feature="clippy", plugin(clippy))]
#![feature(plugin)]

extern crate flate2;
extern crate rusoto;
extern crate crossbeam;
extern crate chrono;
extern crate syslog;
extern crate walkdir;

use std::io;
use std::io::prelude::*;
use std::fs::{File, remove_file, create_dir_all};
use std::path::Path;
use std::env;
use std::str::FromStr;
use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::s3::{S3Client, PutObjectRequest};
use rusoto::default_tls_client;
use flate2::Compression;
use flate2::write::GzEncoder;
use chrono::prelude::Local;
use chrono::{Datelike, Timelike, Duration};
use crossbeam::scope;
use syslog::{Facility, Severity};
use walkdir::WalkDir;

fn main() {

    const MAX_LINES: usize = 50000;
    const MAX_BYTES: usize = 1000000; // 1M (this feels wrong though)
    const MAX_TIMEOUT: i64 = 5; // in seconds

    println!("MAX_LINES: {}    MAX_BYTES: {}    MAX_TIMEOUT: {}",
             MAX_LINES,
             MAX_BYTES,
             MAX_TIMEOUT);

    // attempt to resend any logs that we might have not successfully sent
    resend_logs();

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
                    compress(data.as_slice(), time);
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else {
                    // update the time
                    time = Local::now();
                    // attempt to resend any logs that we might have not successfully sent
                    resend_logs();
                }
            }
            Err(err) => panic!(err),
        }

        // consume the data from the buffer so we don't reprocess it
        buffer.consume(data.len());
    }
}

fn compress(bytes: &[u8], timestamp: chrono::DateTime<chrono::Local>) {
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

            let _ = create_dir_all(&path);

            let mut output = File::create(format!("{}{}", &path, &file)).unwrap();

            let mut encoder = GzEncoder::new(Vec::new(), Compression::Default);

            encoder.write_all(bytes).unwrap();

            let encoded = encoder.finish().unwrap();

            output.write_all(&encoded).unwrap();

            write_s3(&file, &path, &encoded);
        });
    });
}

fn write_s3(file: &str, path: &str, log: &[u8]) {
    // move to new thread
    scope(|scope| {
        scope.spawn(move || {

            let path = format!("{}/{}", &path, &file);

            // set up our credentials provider for aws
            let provider = match DefaultCredentialsProvider::new() {
                Ok(provider) => provider,
                Err(err) => panic!("Unable to load credentials. {}", err),
            };

            // obtain our aws default region so we know where to look
            let region = match env::var("AWS_DEFAULT_REGION") {
                Ok(region) => region,
                Err(err) => panic!("environment AWS_DEFAULT_REGION is not set. {}", err),
            };

            // create our s3 client initialization
            let s3 = S3Client::new(default_tls_client().unwrap(),
                                   provider,
                                   Region::from_str(&region).unwrap());

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            contents.extend(log);

            // if we can read the contents to the buffer we will attempt to send the log to s3
            // need to build our request
            let req = PutObjectRequest {
                bucket: "jkordish-test".to_owned(),
                key: path.to_owned(),
                body: Some(contents),
                ..Default::default()
            };

            match s3.put_object(&req) {
                Ok(_) => {
                    // print notification to stdout.
                    println!("Successfully wrote {}/{}", &req.bucket, &path);
                    // only remove the file if we are successful
                    if remove_file(&path).is_ok() {
                        println!("Removed file {}", &path);
                    }
                }
                Err(e) => println!("Could not write file {} {}", &file, e),
            }

            // Send some notifications to SYSLOG
            match syslog::unix(Facility::LOG_SYSLOG) {
                Err(e) => println!("impossible to connect to syslog: {:?}", e),
                Ok(writer) => {
                    let success = format!("wrote {}/{}", &req.bucket, &path);
                    let failure = format!("unable to write {}/{}", &req.bucket, &path);
                    if writer.send_3164(Severity::LOG_ALERT, &success).is_err() {
                        let _ = writer.send_3164(Severity::LOG_ERR, &failure);
                    }
                }
            }
        });
    });
}

fn resend_logs() {

    // create iterator over the directory
    for entry in WalkDir::new(".").into_iter().filter_map(|e| e.ok()) {
        //let entry = entry.unwrap();

        // filter out gzip'd file suffixes
        if entry.file_name()
               .to_str()
               .map(|s| s.ends_with(".gz"))
               .unwrap() {

            let filename = entry.file_name().to_str().unwrap();

            // need to return only the parent and strip off the prefix ./
            // this then is the path write_s3 is expecting
            // there has to be a better way!
            let path = Path::new(entry.path().parent().unwrap()).strip_prefix("./").unwrap();
            let path = Path::new(path).to_str().unwrap();

            let mut file = File::open(path).expect("Unable to read file");

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            let _ = file.read_to_end(&mut contents);

            println!("Found unsent log {}/{}", &path, &filename);
            write_s3(filename, path, &contents);

        }
    }
}
