# s3post.rs
Take logs from stdin then compress and send to S3.

## config
```json
{
  "cachedir": "/tmp",
  "logfile": "/tmp/s3post.log",
  "role_arn": "arn:aws:iam::<account>:role/<name>",
  "region": "us-west-2",
  "bucket": "<bucket>",
  "prefix": "logs",
  "cbid": "HOSTNAME",
  "ip": "0.0.0.0"
}
```

### todo
- best to have the cbid and ip as both std::option
- add metadata to the S3 upload
- have the aws credentials utilize sts in favor of env vars
