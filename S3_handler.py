import boto3
import os


class S3Handler:
    def __init__(self, bucket_name, file_name, name, value):
        self.s3_session = boto3.session.Session()
        self.s3_client = self.s3_session.client("s3")
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.path = os.path.join(os.getcwd(), self.bucket_name, 'extract')
        self.name = name
        self.value = value


    def upload_s3(self: object) -> None:
        """
        mock uploading file to s3 bucket

        """
        path_to_file = os.path.join(self.path, self.file_name)
        self.s3_client.upload_file(
            path_to_file,
            self.bucket_name,
            self.file_name
        )

    def save(self: object) -> None:
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=self.bucket_name, Key=self.name, Body=self.value)