#!/usr/bin/env python
import boto3
import sys
import gzip
import StringIO
import json
import tempfile
import logging
from threading import Thread
from time import time
from datetime import datetime

class Uploader:
    def __init__(self, conf_file):
        self.MAX_LINES = 50000
        self.MAX_BYTES = 10 * 1024 * 1024
        self.MAX_TIMEOUT = 5

        self.conf = json.load(open(conf_file))

        logging.basicConfig(
            format='%(levelname)s %(thread)d %(asctime)s %(message)s',
            level=logging.INFO,
            filename=self.conf["logfile"]
        )

        self.log = logging.getLogger()
        self.threads = []

    def updateCreds(self):
        self.sts_client = boto3.client('sts')
        self.assumedRole = self.sts_client.assume_role(
            RoleArn=self.conf["RoleArn"],
            RoleSessionName="s3post"
        )

        self.credentials = self.assumedRole['Credentials']

        self.s3 = boto3.client(
            's3',
            aws_access_key_id = self.credentials['AccessKeyId'],
            aws_secret_access_key = self.credentials['SecretAccessKey'],
            aws_session_token = self.credentials['SessionToken'],
            region_name=self.conf["region"]
            )

    def upload(self, buf, counter=0, fmt="raw"):
        # Get a new logger in this thread
        self.log = logging.getLogger()

        try:
            self.updateCreds()
            buf.seek(0)
            buf_bytes = buf.read().encode('utf-8')
            data_length = len(buf_bytes)
            gz_buf = StringIO.StringIO()
            gzfh = gzip.GzipFile(fileobj=gz_buf, mode='wb')
            gzfh.write(buf_bytes)
            gzfh.close()
            gz_buf.seek(0)

            key_name="%s/%s-%s.%s.gz" % (self.conf["bucket_prefix"], datetime.now().strftime("%Y/%m/%d/%H/%M/%S.%f"), self.conf["cbid"], fmt)
            start = time()
            response = self.s3.put_object(
                Bucket=self.conf["bucket"],
                Body=gz_buf,
                Key=key_name,
                Metadata={
                    "cbid": self.conf["cbid"],
                    "ip": self.conf["ip"],
                    "uncompressed_bytes": str(data_length),
                    "lines": str(counter)
                }
            )
            if not response:
                buf.rollover()
                self.log.error("Failed to write to S3, file is located at %s" % buf.name)
            else:
                self.log.info("Finished uploading key %s in %f" % (key_name, time() - start))
        except Exception as e:
            self.log.exception("Failed to write to S3", exc_info=e)
            buf.rollover()

    def get_buf(self):
        return tempfile.SpooledTemporaryFile(dir=self.conf["tempdir"])

    def read(self, fh):
        last_time = time()
        counter = 0
        bytes = 0
        buf = self.get_buf()
        self.log.info("starting up")
        for line in fh:
            counter += 1
            bytes += len(line)
            buf.write(line)
            since_last = int(time() - last_time)
            if counter >= self.MAX_LINES or bytes >= self.MAX_BYTES or since_last >= self.MAX_TIMEOUT:
                self.log.info("Uploading at %d lines, %d bytes, %d time" % (counter, bytes, since_last))
                t = Thread(target=self.upload, args=(buf,counter))
                t.start()
                self.threads.append(t)
                bytes = 0
                counter = 0
                last_time = time()
                buf = self.get_buf()

        if counter > 0:
            self.log.info("Uploading at %d lines, %d bytes, %d time" % (counter, bytes, since_last))
            t = Thread(target=self.upload, args=(buf,counter))
            t.start()
            self.threads.append(t)
        # join all threads
        for t in self.threads:
            t.join()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    uploader = Uploader(conf_file)
    uploader.read(sys.stdin)
