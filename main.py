import logging
import os

from decouple import config

from extract import Extract
from transform import Transform
from mock_s3 import S3mocking

url = config('URL')
file_name = config('FILE_NAME')
bucket_name = config('BUCKET_NAME')
key = config('KEY')
path = os.path.join(os.getcwd(), bucket_name, key)


logger = logging.getLogger('etl_application')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('ETL.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


def main():
    logger.info("\t EXTRACT")
    # EXTRACT
    extraction = Extract(url, file_name, bucket_name, path)

    # download files
    extraction.download_url()

    # Upload to S3 (mocking)
    s3_obj = S3mocking(file_name, bucket_name)
    s3_obj.load_save()

    # TRANSFORM
    logger.info("\t TRANSFORM")
    pfn = Transform(path, file_name).main()

    # LOAD
    s3_load = S3mocking(pfn, bucket_name)
    logger.info("\t LOAD")
    s3_load.load_save()


if __name__ == '__main__':
    main()
