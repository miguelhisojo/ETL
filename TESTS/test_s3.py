import boto3
import pytest

from moto import mock_s3
from decouple import config

from ..load import Load

S3_KEY = config('KEY')
S3_BUCKET = config('BUCKET_NAME')


@pytest.fixture(scope="function")
def _s3():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=S3_BUCKET)
        versioning = conn.BucketVersioning(S3_KEY)
        versioning.enable()
        yield {"client": boto3.client("s3", region_name="us-east-1"), "resource": conn}


def test_get_meta_file(_s3):
    assert Load.get_meta_file(S3_BUCKET.join("/test.txt")) == []


def test_list_versions(_s3, mocker):
    _s3["client"].put_object(
        Bucket="extract", Key=S3_BUCKET.join("/test.txt"), Body="scan stl file",
    )
    expected = [{"created": mocker.ANY, "id": mocker.ANY}]

    assert Load.list_versions(S3_BUCKET.join("/test.txt")) == expected

    _s3["client"].put_object(
        Bucket="extract", Key=S3_BUCKET.join("/test.txt"), Body="scan stl file",
    )
    expected = [
        {"created": mocker.ANY, "id": mocker.ANY},
        {"created": mocker.ANY, "id": mocker.ANY},
    ]
    assert Load.list_versions(S3_BUCKET.join("/test.txt")) == expected


def test_sort_versions():
    versions = [
        {"id": "kjksdf", "created": "November 25, 2020"},
        {"id": "asdf", "created": "November 24, 2020"},
    ]
    expected = [
        {"id": "asdf", "created": "November 24, 2020"},
        {"id": "kjksdf", "created": "November 25, 2020"},
    ]
    assert Load.sort_versions(versions) == expected


def test_save_file(_s3, mocker):
    versions = Load.save_file(S3_BUCKET, "test2.txt", "Hello World", "first")

    assert versions == [
        {"created": mocker.ANY, "name": "first"},
    ]

    versions = Load.save_file(S3_BUCKET, "test2.txt", "Hello World\nHello World", "second")

    assert versions == [
        {"created": mocker.ANY, "name": "first"},
        {"created": mocker.ANY, "name": "second"},
    ]