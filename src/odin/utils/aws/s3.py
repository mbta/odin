import os
from typing import List
from typing import Tuple
from typing import Optional
from typing import Dict
from functools import lru_cache

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from odin.utils.logger import ProcessLog

S3_POOL_COUNT = 50


@lru_cache
def get_client() -> BaseClient:
    """Thin function needed for stubbing tests"""
    aws_profile = os.getenv("AWS_PROFILE", None)
    client_config = Config(max_pool_connections=S3_POOL_COUNT)

    return boto3.Session(profile_name=aws_profile).client("s3", config=client_config)


def split_object(object: str) -> Tuple[str, str]:
    """
    Split s3 object as "s3://bucket/object_key" into Tuple[bucket, key].

    :param object: s3 object as "s3://bucket/object_key" or "bucket/object_key"

    :return: Tuple[bucket, key]
    """
    bucket, key = object.replace("s3://", "").split("/", 1)

    return (bucket, key)


def list_objects(
    bucket: str,
    prefix: str,
    max_objects: int = 1_000_000,
    in_filter: Optional[str] = None,
) -> List[str]:
    """
    Get list of S3 objects in 'bucket' starting with 'prefix'

    :param bucket: the name of the bucket with objects
    :param prefix: prefix for objs to return
    :param max_objects: maximum number of objects to return
    :param in_filter: will filter for objects containing string

    :return: List[s3://bucket/key, ...]
    """
    logger = ProcessLog(
        "list_objects",
        bucket=bucket,
        prefix=prefix,
        max_objects=max_objects,
        in_filter=in_filter,
    )
    try:
        client = get_client()
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        filepaths = []
        for page in pages:
            if page["KeyCount"] == 0:
                continue
            for obj in page["Contents"]:
                if obj["Size"] == 0:
                    continue
                if in_filter is None or in_filter in obj["Key"]:
                    filepaths.append(os.path.join("s3://", bucket, obj["Key"]))

            if len(filepaths) >= max_objects:
                break

        logger.complete(objects_found=len(filepaths))
        return filepaths

    except Exception as exception:
        logger.failed(exception)
        return []


def object_exists(object: str) -> bool:
    """
    Check if S3 object exists.

    will raise on any error other than "NoSuchKey"

    :param object: Object to check as 's3://bucket/object' or 'bucket/object'

    :return: True if exists, otherwise false
    """
    try:
        client = get_client()
        bucket, key = split_object(object)
        client.head_object(Bucket=bucket, Key=key)
        return True

    except ClientError as exception:
        if exception.response["Error"]["Code"] == "404":
            return False
        raise exception


def upload_file(file_name: str, object: str, extra_args: Optional[Dict] = None) -> bool:
    """
    Upload a local file to an S3 Bucket

    :param file_name: local file path to upload
    :param object: S3 object path as 's3://bucket/object' or 'bucket/object'
    :param extra_agrs: additional upload ags available per: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS

    :return: True if file was uploaded, else False
    """
    upload_log = ProcessLog(
        "upload_file",
        file_name=file_name,
        object=object,
        auto_start=False,
    )
    if isinstance(extra_args, dict):
        upload_log.add_metadata(**extra_args)
    upload_log.start()

    try:
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"{file_name} not found locally")

        bucket, key = split_object(object)

        client = get_client()

        client.upload_file(file_name, bucket, key, ExtraArgs=extra_args)

        upload_log.complete()

        return True

    except Exception as exception:
        upload_log.failed(exception)
        return False


def download_object(object: str, local_path: str) -> bool:
    """
    Download S3 'object' to 'local_path', will overwrite 'local_path' if exists.

    :param object: object to download as 's3://bucket/object' or 'bucket/object'
    :param local_path: local file path to save object to

    :return: True if downloaded, else False
    """
    logger = ProcessLog("download_object", object=object, local_path=local_path)
    try:
        if os.path.exists(local_path):
            os.remove(local_path)

        bucket, key = split_object(object)
        client = get_client()
        client.download_file(bucket, key, local_path)
        logger.complete()
        return True

    except Exception as exception:
        logger.failed(exception)
        return False


def get_object(object: str) -> StreamingBody:
    """
    Get an S3 object as StreamingBody

    :param object: S3 object path as 's3://bucket/object' or 'bucket/object'

    :return: http streaming body response
    """
    logger = ProcessLog("get_object", object=object)
    try:
        bucket, key = split_object(object)
        client = get_client()
        object_stream = client.get_object(Bucket=bucket, Key=key)["Body"]
        logger.complete()
        return object_stream

    except Exception as exception:
        logger.failed(exception=exception)
        raise exception


def delete_object(object: str) -> bool:
    """
    Delete s3 object

    :param object: S3 object to delete as 's3://bucket/object' or 'bucket/object'

    :return: True if delete success, else False
    """
    logger = ProcessLog("delete_object", object=object)
    try:
        client = get_client()
        bucket, key = split_object(object)
        client.delete_object(Bucket=bucket, Key=key)
        logger.complete()
        return True

    except Exception as exception:
        logger.failed(exception)
        return False


def rename_object(from_object: str, to_object: str) -> bool:
    """
    Rename from_object TO to_object as copy and delete operation.

    :param from_object: COPY from as 's3://bucket/object' or 'bucket/object'
    :param to_object: COPY to as 's3://bucket/object' or 'bucket/object'

    :return: True if success, else False
    """
    logger = ProcessLog("rename_object", from_object=from_object, to_object=to_object)
    try:
        client = get_client()
        from_object = os.path.join(*split_object(from_object))
        to_bucket, to_key = split_object(to_object)

        client.copy_object(
            Bucket=to_bucket,
            CopySource=from_object,
            Key=to_key,
        )
        if not delete_object(from_object):
            raise FileExistsError(f"failed to delete {from_object}")

        logger.complete()
        return True

    except Exception as exception:
        logger.failed(exception)

    return False
