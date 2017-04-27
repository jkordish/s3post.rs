#![cfg_attr(feature="clippy", plugin(clippy))]
#![feature(plugin)]

extern crate flate2;
extern crate rusoto;
extern crate crossbeam;
extern crate chrono;
extern crate syslog;

use std::io;
use std::io::prelude::*;
use std::fs::{File, remove_file};
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

fn main() {

    const MAX_LINES: usize = 50000;
    const MAX_BYTES: usize = 1000000; // 1M (this feels wrong though)
    const MAX_TIMEOUT: i64 = 5; // in seconds

    println!("MAX_LINES: {}    MAX_BYTES: {}    MAX_TIMEOUT: {}",
             MAX_LINES,
             MAX_BYTES,
             MAX_TIMEOUT);

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
                    compress(data.as_slice());
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else {
                    // update the time
                    time = Local::now();
                }
            }
            Err(err) => panic!(err),
        }

        // consume the data from the buffer so we don't reprocess it
        buffer.consume(data.len());
    }
}

fn compress(bytes: &[u8]) {
    // our compression routine will be sent to another thread
    scope(|scope| {
        scope.spawn(move || {
            // use our environment variable of hostname. will be essentially the CBID
            let hostname = match env::var("HOSTNAME") {
                Ok(hostname) => hostname,
                Err(err) => panic!("Unable to get hostname. {}", err),
            };

            let timestamp = Local::now();

            // setting the format for how we write out the file.
            let file = format!("{}.{}-{}.raw.gz",
                               timestamp.second(),
                               timestamp.timestamp_subsec_millis(),
                               &hostname);

            let mut output = File::create(&file).unwrap();

            let mut encoder = GzEncoder::new(Vec::new(), Compression::Default);

            encoder.write_all(bytes).unwrap();

            let encoded = encoder.finish().unwrap();

            output.write_all(&encoded).unwrap();

            write_s3(&file, &encoded);
        });
    });
}

fn write_s3(file: &str, log: &[u8]) {
    // move to new thread
    scope(|scope| {
        scope.spawn(move || {

            let timestamp = Local::now();
            let path = format!("{}/{}/{}/{}/{}/{}",
                               timestamp.year(),
                               timestamp.month(),
                               timestamp.day(),
                               timestamp.hour(),
                               timestamp.minute(),
                               &file);

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
                    if remove_file(&file).is_ok() {
                        println!("Removed file {}", &file);
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
