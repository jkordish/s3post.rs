#!/bin/bash

###
# using local musl cross you can do the following
###
# brew install FiloSottile/musl-cross/musl-cross
# brew tap SergioBenitez/osxct
# brew install x86_64-unknown-linux-gnu

###
# found using a docker container pre-setup for musl was the easiest though
###

docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder:nightly cargo build --release

sleep 3

/usr/local/bin/x86_64-unknown-linux-gnu-strip ./target/x86_64-unknown-linux-musl/release/s3post

ls -lh ./target/x86_64-unknown-linux-musl/release/s3post
file ./target/x86_64-unknown-linux-musl/release/s3post