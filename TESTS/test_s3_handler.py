import boto3
from moto import mock_s3
from S3_handler import S3Handler

@mock_s3
def test_upload_s3_file():
    try:
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket='S3_bucket')
        s3_handler = S3Handler('S3_bucket', 'PPR_ALL.zip')
        s3_handler.upload_s3()
    except:
        print("no")