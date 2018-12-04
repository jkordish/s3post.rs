#!/bin/bash

###
# using local musl cross you can do the following
###
# brew install FiloSottile/musl-cross/musl-cross
# brew tap SergioBenitez/osxct
# brew install x86_64-unknown-linux-gnu

# export URL=http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl-dev_1.0.1t-1+deb7u4_amd64.deb
# curl -L -s -O $URL
# ar p $(basename $URL) data.tar.xz | tar xvf -

# export URL=http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl1.1_1.1.0f-3+deb9u2_arm64.deb
# curl -L -s -O $URL
# ar p $(basename $URL) data.tar.xz | tar xvf -

# export URL=http://ftp.us.debian.org/debian/pool/main/g/glibc/libc6_2.24-11+deb9u3_amd64.deb
# curl -L -s -O $URL
# ar p $(basename $URL) data.tar.xz | tar xvf -

# Linker for the target platform
# (cc can also be updated using .cargo/config)
# export TARGET_CC="x86_64-unknown-linux-gnu-gcc"
# export TARGET_CC="x86_64-linux-musl-gcc"


# Library headers to link against
# export TARGET_CFLAGS="-I $(pwd)/usr/include/x86_64-linux-gnu -isystem $(pwd)/usr/include"
# Libraries (shared objects) to link against
# export LD_LIBRARY_PATH="$(pwd)/usr/lib/x86_64-linux-gnu;$(pwd)/lib/x86_64-linux-gnu"

# openssl-sys specific build flags
# export OPENSSL_DIR="$(pwd)/usr"
# export OPENSSL_LIB_DIR="$(pwd)/usr/lib/x86_64-linux-gnu"

# cargo build --release --target x86_64-unknown-linux-musl
# /usr/local/bin/x86_64-unknown-linux-gnu-strip ./target/x86_64-unknown-linux-musl/release/aws_lambda_logs

###
# found using a docker container pre-setup for musl was the easiest though
###

docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder:nightly cargo build --release
/usr/local/bin/x86_64-unknown-linux-gnu-strip ./target/x86_64-unknown-linux-musl/release/s3post