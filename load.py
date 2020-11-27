import boto3
import botocore
import json
from decouple import config

from datetime import datetime
from dateutil import parser

S3_BUCKET = config('BUCKET_NAME')
from decouple import config

def get_meta_file(folder):
    """
    Given a folder , returns meta file contents

    :return: list<dict>
    """
    key = f"{folder}/meta.json"
    S3 = boto3.resource("s3")
    content_object = S3.Object(S3_BUCKET, key)
    try:
        object = content_object.get()
    except botocore.exceptions.ClientError:
        return []
    return json.loads(object["Body"].read().decode("utf-8"))


def list_versions(key: str):
    """
    Given a string key, returns list of versions

    :return: list<dict>
    """
    S3 = boto3.resource("s3")
    versions = S3.Bucket(S3_BUCKET).object_versions.filter(Prefix=key)
    results = []
    for version in versions:
        obj = version.get()
        results.append(
            {"id": obj.get("VersionId"), "created": f'{obj.get("LastModified")}'}
        )
    return results


def sort_versions(versions: str):
    """
    Sort list of dictionaries based on created timestamp in the dictionary

    :return: list<dict>
    """
    return sorted(versions, key=lambda item: parser.parse(item["created"]))


def update_meta_file(folder, name, version_id):
    """
    Given a folder and name of version with version id. Saves the version into meta file

    :return: list<dict>
    """
    S3 = boto3.resource("s3")
    key = f"{folder}/meta.json"
    versions = get_meta_file(folder)
    versions.append({"name": name, "created": f"{datetime.now()}"})
    object = S3.Object(S3_BUCKET, key)
    object.put(Body=json.dumps(versions))
    return versions


def save_file(folder, file_name, content, name):
    """
    Given a folder, file_name and content and version name, saves the file in s3 and updates meta file

    :return: list<dict>
    """
    S3 = boto3.resource("s3")
    key = f"{folder}/{file_name}"
    object = S3.Object(S3_BUCKET, key)
    object.put(Body=content)
    versions = sort_versions(list_versions(key))
    return update_meta_file(folder, name, versions[-1]["id"])