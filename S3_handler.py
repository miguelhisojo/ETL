import os
import boto3


class S3Handler:
    def __init__(self, bucket_name, file_name, name, value):
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.path = os.path.join(os.getcwd(), self.bucket_name, 'extract')
        self.name = name
        self.value = value

    def save(self: object) -> None:
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=self.bucket_name,
                      Key=self.name,
                      Body=self.value)
