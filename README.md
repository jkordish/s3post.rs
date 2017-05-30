# s3post.rs
Take logs from stdin then compress and send to S3.

## config
```json
{
  "cachedir": "/tmp/s3post",
  "role_arn": "arn:aws:iam::<account>:role/<name>",
  "region": "us-west-2",
  "bucket": "<bucket>",
  "prefix": "logs",
  "logfile": "s3post.log"
}
```

Based off of the example config the logfile will be /tmp/s3post/s3post.log

### building

```shell
$ curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly
```

```shell
$ git clone https://ghe.eng.fireeye.com/joseph-kordish/s3post.rs.git
```

```shell
$ cd s3post.rs
$ cargo build --release
```

Binary will be target/release/s3post
