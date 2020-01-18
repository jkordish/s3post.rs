#![cfg_attr(feature = "cargo-clippy", warn(clippy::all))]

use cadence::{prelude::*, BufferedUdpMetricSink, QueuingMetricSink, StatsdClient, DEFAULT_PORT};
use chrono::{prelude::Local, Datelike, Duration, Timelike};
use crossbeam_utils::thread::scope;
use flate2::{write::GzEncoder, Compression};
use rusoto_core::{request::HttpClient, Region};
use rusoto_credential::{AutoRefreshingProvider, DefaultCredentialsProvider};
use rusoto_s3::{PutObjectRequest, S3Client, StreamingBody, S3};
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use serde::{Deserialize, Serialize};
use slog::{crit, error, info, o, Drain};
use std::{
    collections::HashMap,
    env,
    error::Error,
    fs::{create_dir_all, read_to_string, remove_dir, remove_file, File, OpenOptions},
    io,
    io::prelude::*,
    net::UdpSocket,
    path::Path,
    process::exit,
    str::FromStr,
    sync::Arc,
    thread,
    time::Instant
};
use walkdir::WalkDir;

// struct for our config file
#[derive(Serialize, Deserialize, Clone)]
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
#[derive(Clone)]
struct SystemInfo {
    hostname: String,
    ipaddr: String
}

pub struct MetricRequestHandler {
    metrics: Arc<dyn MetricClient + Send + Sync>
}

impl MetricRequestHandler {
    fn new() -> Result<MetricRequestHandler, Box<dyn Error>> {
        let socket: UdpSocket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_nonblocking(true)?;
        let host = ("localhost", DEFAULT_PORT);
        let udp_sink = BufferedUdpMetricSink::from(host, socket)?;
        let queuing_sink = QueuingMetricSink::from(udp_sink);
        Ok(MetricRequestHandler {
            metrics: Arc::new(StatsdClient::from_sink("s3post", queuing_sink))
        })
    }

    fn metric_time(&self, metric: &str, time: u64) -> Result<(), String> {
        let metrics_ref = Arc::clone(&self.metrics);
        let metric = metric.to_string();

        let t = thread::spawn(move || {
            let _ = metrics_ref.time(metric.as_ref(), time);
        });

        t.join().is_ok();
        Ok(())
    }
    fn metric_count(&self, num: i64, metric: &str) -> Result<(), String> {
        let metrics_ref = Arc::clone(&self.metrics);
        let metric = metric.to_string();

        let t = thread::spawn(move || {
            match num {
                0 => {
                    let _ = metrics_ref.count(metric.as_ref(), 0);
                }
                1 => {
                    let _ = metrics_ref.count(metric.as_ref(), 1);
                }
                _ => ()
            };
        });

        t.join().is_ok();
        Ok(())
    }
}

// few consts. might make these configurable later
const MAX_LINES: usize = 50_000;
const MAX_BYTES: usize = 10_485_760;
const MAX_TIMEOUT: i64 = 30;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().collect();
    if args.is_empty() {
        println!("s3post <config.json>");
        exit(1)
    };

    // attempt to deserialize the config to our struct
    let config: ConfigFile = match serde_json::from_str(&read_to_string(&args[1])?) {
        Ok(json) => json,
        Err(_) => {
            println!("{} invalid json?", &args[1]);
            exit(1)
        }
    };

    logging(&config.clone(), "info", "S3POST Starting up!").is_ok();

    // create initial log directory
    // appending /raw to the directory to support raw text logs not from stdin
    let _ = create_dir_all(format!("{}/raw", &config.cachedir));

    let hostname = read_to_string("/etc/hostname")
        .unwrap_or_else(|_| "localhost".to_string())
        .trim()
        .to_string();

    // need the ipaddr of our interface. will be part of the metadata
    let mut ipaddr = String::new();
    for iface in get_if_addrs::get_if_addrs()? {
        if !iface.is_loopback() {
            ipaddr = iface.ip().to_string();
        }
    }

    // store hostname and ip address in our struct
    let system: SystemInfo = SystemInfo { hostname, ipaddr };

    logging(
        &config.clone(),
        "info",
        &format!(
            "MAX_LINES: {}  MAX_BYTES: {}  MAX_TIMEOUT:{}",
            MAX_LINES, MAX_BYTES, MAX_TIMEOUT
        )
    )
    .is_ok();

    logging(
        &config.clone(),
        "info",
        &format!("Hostname: {}  ipAddr: {}", &system.hostname, &system.ipaddr)
    )
    .is_ok();

    // create the cachedir in case it isn't there already
    let _ = create_dir_all(&config.cachedir);

    // attempt to resend any logs that we might have not successfully sent
    resend_logs(&config.clone(), &system.clone())?;

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

    loop {
        match buffer.fill_buf() {
            Ok(bytes) => {
                // add the current bytes to our data vector
                data.extend(bytes);
                // evaluate if data and time meet of processing conditions
                if data.lines().count() >= MAX_LINES
                    || data.len() >= MAX_BYTES
                    || timeout <= time && !data.is_empty()
                {
                    // send the data to the compress function via separate thread
                    scope(|scope| {
                        scope.spawn(|_| {
                            compress(&data.clone(), time, config.clone(), system.clone()).is_ok();
                        });
                    })
                    .is_ok();
                    // clear our data vector
                    data.clear();
                    // reset our timer
                    time = Local::now();
                    timeout = time + Duration::seconds(MAX_TIMEOUT);
                } else {
                    scope(|scope| {
                        // update the time
                        time = Local::now();
                        // attempt to resend any logs that we might have not successfully sent
                        scope.spawn(|_| {
                            resend_logs(&config.clone(), &system.clone()).is_ok();
                        });
                    })
                    .is_ok();
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
    config: ConfigFile,
    system: SystemInfo
) -> Result<(), Box<dyn Error>> {
    let metric = MetricRequestHandler::new()?;

    // our compression routine will be sent to another thread
    // generate our local path
    let path: String = format!(
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
    let _ = create_dir_all(&format!("{}/{}", &config.cachedir, &path));

    // create the file in the full path
    let mut output = File::create(format!("{}/{}", &fullpath, &file))?;

    // create a gzip encoder
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());

    // encode the retrieved bytes
    let elapsed = Instant::now();
    encoder.write_all(bytes).unwrap();
    let elapsed = elapsed.elapsed().as_millis();
    let log = encoder.finish()?;

    // write out the encoded data to our file
    output.write_all(&log)?;

    // dump metrics to statsd
    metric
        .metric_time("log.compress.time", elapsed as u64)
        .is_ok();
    metric.metric_count(1, "log.compress.count").is_ok();

    // move to new thread
    scope(|scope| {
        scope.spawn(move |_| {
            // call write_s3 to send the gzip'd file to s3
            write_s3(&config.clone(), &system.clone(), &file, &path, &log).is_ok();
        });
    })
    .is_ok();
    Ok(())
}

fn write_s3(
    config: &ConfigFile,
    system: &SystemInfo,
    file: &str,
    path: &str,
    log: &[u8]
) -> Result<(), Box<dyn Error>> {
    let metric = MetricRequestHandler::new()?;

    // move to new thread
    let s3path = format!("{}/{}/{}", &config.prefix, &path, &file);

    let credentials = DefaultCredentialsProvider::new();

    // initiate our sts client
    let sts_client = StsClient::new_with(
        HttpClient::new()?,
        credentials?,
        Region::from_str(&config.region)?
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
    #[allow(unused_variables)]
    let auto_sts_provider = match AutoRefreshingProvider::new(sts_provider) {
        Ok(auto_sts_provider) => auto_sts_provider,
        Err(_) => {
            logging(&config.clone(), "crit", "Unable to obtain STS token").is_ok();
            exit(1)
        }
    };

    // create our s3 client initialization
    let s3 = S3Client::new_with(
        HttpClient::new()?,
        auto_sts_provider,
        Region::from_str(&config.region)?
    );

    // generate our metadata which we add to the s3 upload
    let mut metadata = HashMap::new();
    metadata.insert("cbid".to_string(), system.hostname.to_string());
    metadata.insert("ip".to_string(), system.ipaddr.to_string());
    metadata.insert(
        "uncompressed_bytes".to_string(),
        log.to_vec().len().to_string()
    );
    metadata.insert(
        "lines".to_string(),
        log.to_vec().lines().count().to_string()
    );

    // if we can read the contents to the buffer we will attempt to send the log to s3
    // need to build our request
    let req = PutObjectRequest {
        bucket: config.bucket.to_owned(),
        key: s3path.to_owned(),
        body: Some(StreamingBody::from(log.to_vec())),
        metadata: Some(metadata),
        ..Default::default()
    };

    match s3.put_object(req).sync() {
        // we were successful!
        Ok(_) => {
            // write metric to statsd
            metric.metric_count(1, "s3.write").is_ok();
            metric.metric_count(0, "s3.failure").is_ok();

            // send some notifications
            logging(
                &config.clone(),
                "info",
                &format!("Successfully wrote {}/{}", &config.bucket, &s3path)
            )
            .is_ok();
            // only remove the file if we are successful
            let localpath = format!("{}/{}/{}", &config.cachedir, &path, &file);
            if remove_file(&localpath).is_ok() {
                // send some notifications
                logging(
                    &config.clone(),
                    "info",
                    &format!("Removed file {}", &localpath)
                )
            } else {
                // send some notifications
                logging(
                    &config.clone(),
                    "error",
                    &format!("Unable to remove file: {}", &localpath)
                )
            }
        }
        Err(e) => {
            // send some notifications
            let _ = logging(
                &config.clone(),
                "error",
                &format!("Could not write {} to s3! {}", &file, e)
            );
            metric.metric_count(1, "s3.failure").is_ok();
            Ok(())
        }
    }
}

fn resend_logs(config: &ConfigFile, system: &SystemInfo) -> Result<(), Box<dyn Error>> {
    let metric = MetricRequestHandler::new()?;

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
        if entry
            .file_name()
            .to_str()
            .map(|s| s.ends_with(".gz"))
            .unwrap_or(false)
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
            let mut file = File::open(&filepath)?;

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            let _ = file.read_to_end(&mut contents);

            logging(
                &config.clone(),
                "info",
                &format!("Found unsent log {}/{}", &path, &filename)
            )
            .is_ok();
            // pass the unset logs to s3
            metric.metric_count(1, "s3.resend").is_ok();
            write_s3(&config.clone(), &system.clone(), filename, path, &contents).is_ok();
        }
    }

    if config.raw.is_some() {
        // iterate over our raw directory. these should be any text logs
        // these logs don't follow the year/month/date/hour/minute format
        let directory = format!("{}/{}", &config.cachedir, &config.raw.to_owned().unwrap());

        for entry in WalkDir::new(&directory).into_iter().filter_map(|e| e.ok()) {
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
            let mut file = File::open(&filepath).unwrap();

            // create a u8 vector
            let mut contents: Vec<u8> = Vec::new();
            // add our encoded log file to the vector
            let _ = file.read_to_end(&mut contents);

            logging(
                &config.clone(),
                "info",
                &format!("Found unsent log {}/{}", &path, &filename)
            )
            .is_ok();
            // pass the unset logs to s3
            metric.metric_count(1, "s3.resend").is_ok();
            write_s3(&config.clone(), &system.clone(), filename, path, &contents).is_ok();
        }
    };
    Ok(())
}

fn logging(config: &ConfigFile, log_type: &str, msg: &str) -> Result<(), Box<dyn Error>> {
    // log to logfile otherwise to stdout
    let config = &config.clone();

    if config.logfile.is_some() {
        let file = format!(
            "{}/{}",
            &config.cachedir,
            &config.logfile.to_owned().unwrap()
        );

        // have to convert file to a Path
        let path = Path::new(&file).to_str().unwrap();

        let file: File = OpenOptions::new().create(true).append(true).open(path)?;

        let decorator = slog_term::PlainDecorator::new(file);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let logger = slog::Logger::root(drain, o!());
        match log_type {
            "info" => {
                info!(logger, "S3POST"; "[*]" => &msg);
                Ok(())
            }
            "error" => {
                error!(logger, "S3POST"; "[*]" => &msg);
                Ok(())
            }
            "crit" => {
                crit!(logger, "S3POST"; "[*]" => &msg);
                Ok(())
            }
            _ => Ok(())
        }
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let logger = slog::Logger::root(drain, o!());
        match log_type {
            "info" => {
                info!(logger, "S3POST"; "[*]" => &msg);
                Ok(())
            }
            "error" => {
                error!(logger, "S3POST"; "[*]" => &msg);
                Ok(())
            }
            "crit" => {
                crit!(logger, "S3POST"; "[*]" => &msg);
                Ok(())
            }
            _ => Ok(())
        }
    }
}
