import logging
import boto3
from moto import mock_s3
from S3_handler import S3Handler

module_logger = logging.getLogger('etl_application.extract')


class S3mocking:
    def __init__(self, file_name, bucket_name):
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.logger = logging.getLogger('etl_application.mock_s3.S3mocking')
        self.logger.info('creating an instance of S3mocking')

    @mock_s3
    def load_save(self: object) -> None:
        """
        receives object and mocks AWS - S3
        :arg: object
        :return: None
        """
        try:
            self.logger.info(f"\t - uploading {self.file_name} file to S3")
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.bucket_name)
            versioning = conn.BucketVersioning(self.bucket_name)
            versioning.enable()
            model_instance = S3Handler(self.bucket_name,
                                       self.file_name,
                                       'miguel',
                                       'is awesome')
            model_instance.save()

            body = conn.Object(self.bucket_name, 'miguel').get()[
                'Body'].read().decode("utf-8")

            self.logger.info("\t - S3 upload successful")

        except (ValueError, Exception):
            self.logger.exception('error : ', exc_info=True)
