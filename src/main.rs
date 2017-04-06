#![cfg_attr(feature="clippy", plugin(clippy))]
#![feature(plugin)]

extern crate flate2;
extern crate rusoto;
extern crate crossbeam;
extern crate chrono;

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
                if data.lines().count() >= MAX_LINES {
                    println!("Len: {:?} Line Count: {:?}",
                             data.len(),
                             data.lines().count());
                    // send the data to the compress function
                    compress(data.as_slice());
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else if data.len() >= MAX_BYTES {
                    println!("Len: {:?} Line Count: {:?}",
                             data.len(),
                             data.lines().count());
                    // send the data to the compress function
                    compress(data.as_slice());
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else if timeout <= time && !data.is_empty() {
                    println!("Len: {:?} Line Count: {:?}",
                             data.len(),
                             data.lines().count());
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

    // our compression routine will be sent to another thread
    scope(|scope| {
        scope.spawn(move || {
            let mut output = File::create(&file).unwrap();

            let mut encoder = GzEncoder::new(Vec::new(), Compression::Default);

            encoder.write_all(bytes).unwrap();

            let encoded = encoder.finish().unwrap();

            println!("Writing to file: {}", &file);

            output.write_all(&encoded).unwrap();

            write_s3(&file);
        });
    });
}

fn write_s3(file: &str) {

    let timestamp = Local::now();
    let path = format!("{}/{}/{}/{}/{}/{}",
                       timestamp.year(),
                       timestamp.month(),
                       timestamp.day(),
                       timestamp.hour(),
                       timestamp.minute(),
                       &file);
    println!("{}", &path);

    scope(|scope| {
        scope.spawn(move || {

            let provider = match DefaultCredentialsProvider::new() {
                Ok(provider) => provider,
                Err(err) => panic!("Unable to load credentials. {}", err),
            };

            let region = match env::var("AWS_DEFAULT_REGION") {
                Ok(region) => region,
                Err(err) => panic!("environment AWS_DEFAULT_REGION is not set. {}", err),
            };

            let s3 = S3Client::new(default_tls_client().unwrap(),
                                   provider,
                                   Region::from_str(&region).unwrap());

            // Open the gzip'd log file
            let mut log = File::open(&file).unwrap();
            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // if we can read the contents to the buffer we will attempt to send the log to s3
            match log.read_to_end(&mut contents) {
                Err(err) => panic!("Error opening file to send to S3: {}", err),
                Ok(_) => {
                    let req = PutObjectRequest {
                        bucket: "jkordish-test".to_owned(),
                        key: path.to_owned(),
                        body: Some(contents),
                        ..Default::default()
                    };
                    let result = s3.put_object(&req);
                    Some(result);
                    // remove the file after we processed it
                    remove_file(&file);
                }
            }
        });
    });
}
