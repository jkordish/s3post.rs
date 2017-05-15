# s3post.rs
Take logs from stdin then compress and send to S3.

## config
```json
{
  "cachedir": "/tmp",
  "role_arn": "arn:aws:iam::<account>:role/<name>",
  "region": "us-west-2",
  "bucket": "<bucket>",
  "prefix": "logs",
  "cbid": "HOSTNAME",
}
```
