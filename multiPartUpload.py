import threading
import boto3
import os
import uuid 
import sys
from boto3.s3.transfer import TransferConfig

s3_resource = boto3.resource('s3')
def create_bucket_name(bucket_prefix):
    return ''.join([bucket_prefix, str(uuid.uuid4())])

def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': current_region})
    return bucket_name, bucket_response

BUCKET_NAME, response = create_bucket(bucket_prefix='pocbucket', s3_connection=s3_resource)

def multi_part_upload_with_s3():
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = os.path.dirname(__file__) + 'AWS_Certified_Solutions_Architect_Offici.pdf'
    key_path = 'multipart_files/largefile.pdf'
    s3_resource.meta.client.upload_file(file_path, BUCKET_NAME, key_path,
                            ExtraArgs={'ContentType': 'text/pdf', 'ServerSideEncryption': 'AES256'},
                            Config=config,
                            Callback=ProgressPercentage(file_path)
                            )

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

if __name__ == '__main__': 
    multi_part_upload_with_s3()