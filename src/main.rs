#![cfg_attr(feature="clippy", plugin(clippy))]
#![feature(custom_derive, plugin, custom_attribute)]

#[macro_use]
extern crate serde_json;
extern crate flate2;
extern crate rusoto;
extern crate crossbeam;

use std::io::prelude::*;
use std::fs::File;
use self::crossbeam::*;
use std::io;
use std::default::Default;
use std::env;
use std::str::FromStr;
use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::s3::{S3Client, GetObjectRequest, PutObjectRequest};
use rusoto::default_tls_client;
use flate2::Compression;
use flate2::write::GzEncoder;

fn main() {

    const MAX_LINES: i32 = 50000;
    const MAX_BYTES: usize = 1000000; // 1 megabyte
    const MAX_TIMEOUT: i32 = 5;

    println!("MAX_LINES: {}    MAX_BYTES: {}    MAX_TIMEOUT: {}",
             MAX_LINES,
             MAX_BYTES,
             MAX_TIMEOUT);

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

    let buckets = s3.list_buckets().unwrap();

    //for bucket in buckets.buckets.unwrap() {
    //    println!("{:?}", &bucket.name.unwrap());
    //}

    let reader = io::stdin();
    let mut buffer = reader.lock();

    loop {
        let consumed =
            match buffer.fill_buf() {
                Ok(bytes) => {
                    println!("bytes length: {:?}", bytes.len());
                    if bytes.len() >= MAX_BYTES {
                        crossbeam::scope(|scope| {
                                             scope.spawn(move || {
                                println!("{:?} have reached max of {:?}", bytes.len(), MAX_BYTES);
                                compress(bytes)
                            });
                                         });

                    } else {
                    };
                    bytes.len()
                }
                Err(ref err) => panic!("Failed with: {}", err),
            };
        //buffer.consume(consumed);
    }
}

fn compress(bytes: &[u8]) {

    println!("Entered compress function");

    let mut output = File::create("foo.gz").unwrap();

    println!("Bytes are {:?}", bytes.len());

    let mut encoder = GzEncoder::new(Vec::new(), Compression::Default);

    encoder.write(bytes).unwrap();

    let encoded = encoder.finish().unwrap();

    println!("Writing to file");

    output.write_all(&encoded).unwrap();
}
