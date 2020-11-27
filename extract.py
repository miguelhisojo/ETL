import os
import boto3
import requests
from moto import mock_s3
from S3_handler import S3Handler
from zipfile import ZipFile

class Extract:
    def __init__(self, url, file_name, bucket_name, path):
        self.url = url
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.path = path

    @mock_s3
    def test_my_model_save(self):
        try:
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.bucket_name)

            model_instance = S3Handler(self.bucket_name, self.file_name, 'miguel', 'is awesome')
            model_instance.save()

            body = conn.Object(self.bucket_name, 'miguel').get()[
                'Body'].read().decode("utf-8")

            print("put success")
            assert body == 'is awesome'
        except Exception as e:
            print('error : ', e)

    def download_url(self):

        print(f"Extracting file {self.file_name} from {self.url} \n")
        zip_file = os.path.join(self.path, self.file_name)

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)

            r = requests.get(self.url, verify=False, timeout=3)
            r.raise_for_status()

            if r.status_code == 200:
               open(zip_file, 'wb').write(r.content)

               with ZipFile(zip_file, 'r') as zipObj:
                   zipObj.extractall(self.path)

               print(f"file {zip_file} successfully extracted \n")

            else:
                print('Web site does not exist')
        except requests.exceptions.RequestException as err:
            print("Exception: Other Errors: ", err)
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)

    @mock_s3
    def upload_s3(self):
        print(f"Uploading file {self.file_name} to s3 bucket {self.bucket_name} \n ")
        try:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket='S3_bucket')
            s3_handler = S3Handler('S3_bucket', 'PPR_ALL.zip')
            s3_handler.upload_s3()
            print("upload successful \n ")
        except:
            print("error: ")



