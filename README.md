# s3post.rs
Take logs from stdin then compress and send to S3.

Ideally you could couple this with a syslog remote server for the collection of all logs from systems pointed at it.

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
$ git clone https://github.com/jkordish/s3post.rs.git
```

```shell
$ cd s3post.rs
$ cargo build --release
```

Binary will be target/release/s3post

### simple install
```shell
$ curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain nightly
```

```shell
$ cargo install --git https://github.com/jkordish/s3post.rs.git
```
