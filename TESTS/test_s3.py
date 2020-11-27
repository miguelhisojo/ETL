import boto3
import pytest

from moto import mock_s3


@pytest.fixture(scope="function")
def _s3():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="extract")
        versioning = conn.BucketVersioning("extract")
        versioning.enable()
        yield {"client": boto3.client("s3", region_name="us-east-1"), "resource": conn}


from ..load import (list_versions,  sort_versions, get_meta_file, save_file)


def test_get_meta_file(_s3):
    assert get_meta_file("extract/test.txt") == []


def test_list_versions_happy_path(_s3, mocker):
    _s3["client"].put_object(
        Bucket="extract", Key="extract/test.txt", Body="scan stl file",
    )
    expected = [{"created": mocker.ANY, "id": mocker.ANY}]

    assert list_versions("extract/test.txt") == expected

    _s3["client"].put_object(
        Bucket="extract", Key="extract/test.txt", Body="scan stl file",
    )
    expected = [
        {"created": mocker.ANY, "id": mocker.ANY},
        {"created": mocker.ANY, "id": mocker.ANY},
    ]
    assert list_versions("extract/test.txt") == expected


def test_sort_versions():
    versions = [
        {"id": "kjksdf", "created": "November 25, 2020"},
        {"id": "asdf", "created": "April 24, 2020"},
    ]
    expected = [
        {"id": "asdf", "created": "April 24, 2020"},
        {"id": "kjksdf", "created": "April 25, 2020"},
    ]
    assert sort_versions(versions) == expected



def test_save_file(_s3, mocker):
    versions = save_file("extract", "test2.txt", "Hello World", "first")

    assert versions == [
        {"created": mocker.ANY, "name": "first"},
    ]

    versions = save_file("extract", "test2.txt", "Hello World\nHello World", "second")

    assert versions == [
        {"created": mocker.ANY, "name": "first"},
        {"created": mocker.ANY, "name": "second"},
    ]