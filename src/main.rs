#![cfg_attr(feature="clippy", plugin(clippy))]
#![feature(plugin)]

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate flate2;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate rusoto_sts;
extern crate crossbeam;
extern crate chrono;
extern crate walkdir;
extern crate ifaces;
extern crate cadence;
extern crate elapsed;

use std::sync::Arc;
use std::thread;
use std::io;
use std::io::prelude::*;
use std::fs::{File, remove_file, remove_dir, create_dir_all, OpenOptions, metadata};
use slog::Drain;
use std::path::Path;
use std::env;
use std::str::FromStr;
use rusoto_core::{AutoRefreshingProvider, default_tls_client, DefaultCredentialsProvider, Region};
use rusoto_sts::{StsClient, StsAssumeRoleSessionCredentialsProvider};
use rusoto_s3::{S3Client, PutObjectRequest, Metadata, S3};
use flate2::Compression;
use flate2::write::GzEncoder;
use chrono::prelude::Local;
use chrono::{Datelike, Timelike, Duration};
use std::time::UNIX_EPOCH;
use crossbeam::scope;
use walkdir::WalkDir;
use elapsed::measure_time;
use std::net::UdpSocket;
use cadence::prelude::*;
use cadence::{StatsdClient, BufferedUdpMetricSink, DEFAULT_PORT};

// struct for our config file
#[derive(Serialize, Deserialize)]
struct ConfigFile {
    cachedir: String,
    role_arn: String,
    region: String,
    bucket: String,
    prefix: String,
    logfile: Option<String>,
    raw: Option<String>
}

// struct for our system information
struct SystemInfo {
    hostname: String,
    ipaddr: String
}


pub struct MetricRequestHandler {
    metrics: Arc<MetricClient + Send + Sync>
}

impl MetricRequestHandler {
    fn new() -> MetricRequestHandler {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap() ;
        let host = ("localhost", DEFAULT_PORT);
        let sink = BufferedUdpMetricSink::from(host, socket).unwrap();
        MetricRequestHandler {
            metrics: Arc::new(StatsdClient::from_sink("s3post", sink))
        }
    }

    fn metric_time(&self, metric: String, time: std::time::Duration) -> Result<(), String> {

        let metrics_ref = self.metrics.clone();
        let metric = metric.clone();

        let t =
            thread::spawn(move || { let _ = metrics_ref.time_duration(metric.as_ref(), time); });

        t.join().unwrap();
        Ok(())
    }
    fn metric_count(&self, metric: String) -> Result<(), String> {

        let metrics_ref = self.metrics.clone();
        let metric = metric.clone();

        let t = thread::spawn(move || { let _ = metrics_ref.incr(metric.as_ref()); });

        t.join().unwrap();
        Ok(())
    }
}


// few consts. might make these configurable later
const MAX_LINES: usize = 50000;
const MAX_BYTES: usize = 10485760;
const MAX_TIMEOUT: i64 = 30;

fn main() {

    let args: Vec<_> = env::args().collect();
    if args.len() < 1 {
        println!("s3post <config.json>");
    }

    // open our config file
    let file_open = File::open(&args[1]).expect("could not open file");
    // attempt to deserialize the config to our struct
    let config: ConfigFile = serde_json::from_reader(file_open).expect("config has invalid json");

    let message: String = "S3POST Starting up!".to_owned();
    logging(&config, &message);

    // create initial log directory
    // appending /raw to the directory to support raw text logs not from stdin
    let _ = create_dir_all(format!("{}/raw", &config.cachedir));

    // determine our hostname.
    #[allow(unused_assignments)]
    let mut hostname = String::new();
    if let Ok(mut file) = File::open("/etc/hostname") {
        let mut buffer = String::new();
        let _ = file.read_to_string(&mut buffer).unwrap().to_string();
        hostname = buffer.trim().to_owned();
    } else {
        hostname = "localhost".to_string();
    }

    // need the ipaddr of our interface. will be part of the metadata
    let mut address = String::new();
    for iface in ifaces::Interface::get_all().unwrap() {
        if iface.kind == ifaces::Kind::Ipv4 {
            address = format!("{}", iface.addr.unwrap());
            address = address.replace(":0", "");
        }
        continue;
    }

    // store hostname and ip address in our struct
    let system: SystemInfo = SystemInfo {
        hostname: hostname,
        ipaddr: address
    };

    //
    let message = format!(
        "MAX_LINES: {}  MAX_BYTES: {}  MAX_TIMEOUT:{}",
        MAX_LINES,
        MAX_BYTES,
        MAX_TIMEOUT
    );
    logging(&config, &message);

    let message = format!("Hostname: {}  ipAddr: {}", &system.hostname, &system.ipaddr);
    logging(&config, &message);

    // attempt to resend any logs that we might have not successfully sent
    resend_logs(&config, &system);

    // create a reader from stdin
    let reader = io::stdin();
    // create a buffer from the stdin lock
    let mut buffer = reader.lock();
    // initialize an empty vector
    let mut data = vec![0];
    // grab the current time
    let mut time = Local::now();
    // create initial timeout
    let mut timeout = time + Duration::seconds(MAX_TIMEOUT);

    // set new metric thread handler
    let metric = MetricRequestHandler::new();

    loop {
        match buffer.fill_buf() {
            Ok(bytes) => {
                // add the current bytes to our data vector
                data.extend(bytes);
                // evaulate if data and time meet of processing conditions
                if data.lines().count() >= MAX_LINES || data.len() >= MAX_BYTES ||
                    timeout <= time && !data.is_empty()
                {
                    // send the data to the compress function
                    metric.metric_count("log.collect".to_string()).unwrap();
                    compress(data.as_slice(), time, &config, &system);
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else {
                    // update the time
                    time = Local::now();
                    // attempt to resend any logs that we might have not successfully sent
                    resend_logs(&config, &system);
                }
            }
            Err(err) => panic!(err)
        }
        // consume the data from the buffer so we don't reprocess it
        buffer.consume(data.len());
    }
}

fn compress(
    bytes: &[u8],
    timestamp: chrono::DateTime<chrono::Local>,
    config: &ConfigFile,
    system: &SystemInfo
) {

    let metric = MetricRequestHandler::new();

    // our compression routine will be sent to another thread
    scope(|scope| {
        scope.spawn(move || {

            // generate our local path
            let path = format!(
                "{}/{}/{}/{}/{}",
                timestamp.year(),
                timestamp.month(),
                timestamp.day(),
                timestamp.hour(),
                timestamp.minute()
            );

            // setting the format for how we write out the file.
            let file = format!(
                "{}.{}-{}.raw.gz",
                timestamp.second(),
                timestamp.timestamp_subsec_millis(),
                &system.hostname
            );

            // append the cachedir so we write to the correct place
            let fullpath = format!("{}/{}", &config.cachedir, &path);

            // create the directory structure
            let _ = create_dir_all(&fullpath);

            // create the file in the full path
            let mut output = File::create(format!("{}/{}", &fullpath, &file)).unwrap();

            // create a gzip encoder
            let mut encoder = GzEncoder::new(Vec::new(), Compression::Default);

            // encode the retrieved bytes
            let (elapsed, _) = measure_time(|| encoder.write_all(bytes).unwrap());
            let log = encoder.finish().unwrap();

            // write out the encoded data to our file
            output.write_all(&log).unwrap();

            // dump metrics to statsd
            metric.metric_time("log.compress.time".to_string(), elapsed.duration())
                  .unwrap();
            metric.metric_count("log.compress.count".to_string())
                  .unwrap();

            // call write_s3 to send the gzip'd file to s3
            write_s3(&config, &system, &file, &path, &log);
        });
    });
}

fn write_s3(config: &ConfigFile, system: &SystemInfo, file: &str, path: &str, log: &[u8]) {
    let metric = MetricRequestHandler::new();

    // move to new thread
    scope(|scope| {
        scope.spawn(move || {

            let s3path = format!("{}/{}/{}", &config.prefix, &path, &file);

            // set up our credentials provider for aws
            let provider = match DefaultCredentialsProvider::new() {
                Ok(provider) => provider,
                Err(err) => panic!("Unable to load credentials. {}", err)
            };

            // initiate our sts client
            let sts_client = StsClient::new(
                default_tls_client().unwrap(),
                provider,
                Region::from_str(&config.region).unwrap()
            );
            // generate a sts provider
            let sts_provider = StsAssumeRoleSessionCredentialsProvider::new(
                sts_client,
                config.role_arn.to_owned(),
                "s3post".to_owned(),
                None,
                None,
                None,
                None
            );
            // allow our STS to auto-refresh
            let auto_sts_provider = AutoRefreshingProvider::with_refcell(sts_provider);

            // create our s3 client initialization
            let s3 = S3Client::new(
                default_tls_client().unwrap(),
                auto_sts_provider.unwrap(),
                Region::from_str(&config.region).unwrap()
            );

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            contents.extend(log);

            // generate our metadata which we add to the s3 upload
            let mut metadata = Metadata::new();
            metadata.insert("cbid".to_string(), system.hostname.to_string());
            metadata.insert("ip".to_string(), system.ipaddr.to_string());
            metadata.insert("uncompressed_bytes".to_string(), contents.len().to_string());
            metadata.insert("lines".to_string(), contents.lines().count().to_string());

            // if we can read the contents to the buffer we will attempt to send the log to s3
            // need to build our request
            let req = PutObjectRequest {
                bucket: config.bucket.to_owned(),
                key: s3path.to_owned(),
                body: Some(contents),
                metadata: Some(metadata),
                ..Default::default()
            };

            match s3.put_object(&req) {
                // we were successful!
                Ok(_) => {
                    // write metric to statsd
                    metric.metric_count("s3.write".to_string()).unwrap();
                    // send some notifications
                    let message = format!("Successfully wrote {}/{}", &req.bucket, &s3path);
                    logging(&config, &message);
                    // only remove the file if we are successful
                    let localpath = format!("{}/{}/{}", &config.cachedir, &path, &file);
                    if remove_file(&localpath).is_ok() {
                        // send some notifications
                        let message = format!("Removed file {}", &localpath);
                        logging(&config, &message);
                    } else {
                        // send some notifications
                        let message = format!("Unable to remove file: {}", &localpath);
                        logging(&config, &message);
                    }
                }
                Err(e) => {
                    // send some notifications
                    let message = format!("Could not write {} to s3! {}", &file, e);
                    logging(&config, &message);
                    metric.metric_count("s3.failure".to_string()).unwrap();
                }
            }
        });
    });
}

fn resend_logs(config: &ConfigFile, system: &SystemInfo) {

    let metric = MetricRequestHandler::new();

    // prune empty directories as overtime we may exhaust inodes
    for entry in WalkDir::new(&config.cachedir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.depth() > 1 {
            let path = Path::new(entry.path()).to_str().unwrap();
            remove_dir(path).is_ok();
        }
    }

    // create iterator over the directory
    // this is for the expected gzip's found from stdin which were compressed previously
    for entry in WalkDir::new(&config.cachedir)
        .into_iter()
        .filter_map(|e| e.ok())
    {

        // filter out gzip'd file suffixes
        if entry.file_name()
                .to_str()
                .map(|s| s.ends_with(".gz"))
                .unwrap()
        {

            // get just the file name
            let filename = entry.file_name().to_str().unwrap();

            // need to return only the parent and strip off the cachedir prefix
            // this then is the path write_s3 is expecting as it is the path within s3
            let path = Path::new(entry.path().parent().unwrap())
                .strip_prefix(&config.cachedir)
                .unwrap();
            let path = Path::new(path).to_str().unwrap();

            // build out the complete path for reading
            let filepath = format!("{}/{}/{}", &config.cachedir, &path, &filename);
            // open the file for reading
            let mut file = File::open(&filepath).expect("Unable to read file");

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            let _ = file.read_to_end(&mut contents);

            let message = format!("Found unsent log {}/{}", &path, &filename);
            logging(&config, &message);
            // pass the unset logs to s3
            metric.metric_count("s3.resend".to_string()).unwrap();
            write_s3(&config, &system, filename, path, &contents);
        }
    }

    if config.raw.is_some() {
        // iterate over our raw directory. these should be any text logs
        // these logs don't follow the year/month/date/hour/minute format
        for entry in WalkDir::new(format!("{}/{}", &config.cachedir, &config.raw.to_owned().unwrap()))
            .into_iter()
            .filter_map(|e| e.ok())
        {
            // get just the file name
            let filename = entry.file_name().to_str().unwrap();
            // get the metadata of the file
            let metadata = metadata(&filename).unwrap();
            // get the date the file was created
            let std_duration = metadata.created().unwrap();
            // convert it to UNIX_EPOCH
            let duration = std_duration.duration_since(UNIX_EPOCH).unwrap();
            // now we can convert SystemTime to something Chrono can use
            let chrono_duration = Duration::from_std(duration).unwrap();
            let unix_time = chrono::NaiveDateTime::from_timestamp(0, 0);
            let naive = unix_time + chrono_duration;

            let datetime = format!(
                "{}/{}/{}/{}/{}",
                naive.year(),
                naive.month(),
                naive.day(),
                naive.hour(),
                naive.minute()
            );

            println!("{:?}", &datetime);

            let filename = entry.file_name().to_str().unwrap();

            // need to return only the parent and strip off the cachedir prefix
            // this then is the path write_s3 is expecting as it is the path within s3
            let path = Path::new(entry.path().parent().unwrap())
                .strip_prefix(&config.cachedir)
                .unwrap();
            let path = Path::new(path).to_str().unwrap();

            // build out the complete path for reading
            let filepath = format!("{}/{}/{}", &config.cachedir, &path, &filename);
            // open the file for reading
            let mut file = File::open(&filepath).expect("Unable to read file");

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            let _ = file.read_to_end(&mut contents);

            let message = format!("Found unsent log {}/{}", &path, &filename);
            logging(&config, &message);
            // pass the unset logs to s3
            metric.metric_count("s3.resend".to_string()).unwrap();
            write_s3(&config, &system, filename, path, &contents);
        }
    }
}

fn logging(config: &ConfigFile, msg: &str) {

    // log to logfile otherwise to stdout
    if config.logfile.is_some() {

        let file = format!("{}/{}", &config.cachedir, &config.logfile.to_owned().unwrap());

        // have to convert file to a Path
        let path = Path::new(&file).to_str().unwrap();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();

        let decorator = slog_term::PlainDecorator::new(file);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let logger = slog::Logger::root(drain, o!());
        info!(logger, "S3POST"; "message:" => &msg);
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let logger = slog::Logger::root(drain, o!());
        info!(logger, "S3POST"; "message:" => &msg);
    }
}
