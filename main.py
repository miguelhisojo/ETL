from extract import Extract
from transform import Transform
import os
from decouple import config


url =config('URL')
file_name = config('FILE_NAME')
bucket_name = config('BUCKET_NAME')
key = config('KEY')
path = os.path.join(os.getcwd(), bucket_name, key)



def main():

    # EXTRACT
    extraction = Extract(url, file_name, bucket_name, path)

    # download files
    extraction.download_url()

    # Upload to S3 (mocking)
    extraction.test_my_model_save()


    # TRANSFORM
    Transform(path).main()

    #LOAD
    extraction.test_my_model_save()
    # load.save_file() need to finish implementation



if __name__ == '__main__':
    main()
