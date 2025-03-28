import os
import datetime
from typing import List
from typing import Tuple
from typing import Optional
from typing import Dict
from typing import NamedTuple
from collections.abc import Callable
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from odin.utils.logger import ProcessLog
from odin.utils.runtime import thread_cpus

S3_POOL_COUNT = 30 * thread_cpus()


class S3Object(NamedTuple):
    """S3 Object return tuple"""

    path: str
    last_modified: datetime.datetime
    size_bytes: int


@lru_cache
def get_client() -> BaseClient:
    """Thin function needed for stubbing tests"""
    aws_profile = os.getenv("AWS_PROFILE", None)
    client_config = Config(max_pool_connections=S3_POOL_COUNT)

    return boto3.Session(profile_name=aws_profile).client("s3", config=client_config)


def split_object(object: str) -> Tuple[str, str]:
    """
    Split S3 object as "s3://bucket/object_key" into Tuple[bucket, key].

    :param object: s3 object as "s3://bucket/object_key" or "bucket/object_key"

    :return: Tuple[bucket, key]
    """
    bucket, key = object.replace("s3://", "").split("/", 1)

    return (bucket, key)


def list_objects(
    partition: str,
    max_objects: int = 1_000_000,
    in_filter: Optional[str] = None,
    in_func: Optional[Callable[[S3Object], bool]] = None,
) -> List[S3Object]:
    """
    Get list of S3 objects starting with 'partition'.

    :param partition: S3 partition as "s3://bucket/prefix" or "bucket/prefix"
    :param max_objects: (Optional) maximum number of objects to return
    :param in_filter: (Optional) will filter for objects containing string
    :param in_func:
        (Optional) function that accepts S3Object and returns bool
        return True to include Key in results or False to exclude from results

    :return: List[s3://bucket/key, ...]
    """
    logger = ProcessLog(
        "list_objects",
        partition=partition,
        max_objects=max_objects,
        in_filter=in_filter,
    )
    bucket, prefix = split_object(partition)
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
                if isinstance(in_filter, str) and in_filter not in obj["Key"]:
                    continue
                append_obj = S3Object(
                    path=os.path.join("s3://", bucket, obj["Key"]),
                    last_modified=obj["LastModified"],
                    size_bytes=obj["Size"],
                )
                if callable(in_func) and in_func(append_obj) is False:
                    continue
                filepaths.append(append_obj)

            if len(filepaths) >= max_objects:
                break

        logger.complete(objects_found=len(filepaths))
        return filepaths

    except Exception as exception:
        logger.failed(exception)
        return []


def list_partitions(
    partition: str,
    max_objects: int = 10_000,
) -> List[str]:
    """
    Get list of S3 "partitions/folders" starting with 'partition'.

    S3 doesn't really have "folders" so this is a bit of a hack

    :param partition: S3 partition as "s3://bucket/prefix" or "bucket/prefix"
    :param max_objects: maximum number of partitions to return

    :return: List[folder, ...]
    """
    partitions = []
    logger = ProcessLog(
        "list_partitions",
        partition=partition,
        max_objects=max_objects,
    )
    bucket, prefix = split_object(partition)
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"
    try:
        client = get_client()
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")

        for page in pages:
            if "CommonPrefixes" in page:
                for part in page["CommonPrefixes"]:
                    partitions.append(str(part["Prefix"]).replace(prefix, "").strip("/"))

            if len(partitions) >= max_objects:
                break
        logger.complete(partitions_found=len(partitions))

    except Exception as exception:
        logger.failed(exception)

    return partitions


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
    Upload a local file to S3 as an object.

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
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
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


def stream_object(object: str) -> StreamingBody:
    """
    Stream an S3 object as StreamingBody.

    :param object: S3 object path as 's3://bucket/object' or 'bucket/object'

    :return: http streaming body response
    """
    logger = ProcessLog("stream_object", object=object)
    try:
        bucket, key = split_object(object)
        client = get_client()
        object_stream = client.get_object(Bucket=bucket, Key=key)["Body"]
        return object_stream

    except Exception as exception:
        logger.failed(exception=exception)
        raise exception


def delete_object(del_object: str) -> Optional[str]:
    """
    Delete an S3 object.

    :param del_object: S3 object to delete as 's3://bucket/object' or 'bucket/object'

    :return: 'del_object' if failed, else None
    """
    logger = ProcessLog("delete_object", del_object=del_object)
    try:
        client = get_client()
        bucket, key = split_object(del_object)
        client.delete_object(Bucket=bucket, Key=key)
        logger.complete()
        return None

    except Exception as exception:
        logger.failed(exception)

    return del_object


def delete_objects(objects: List[str]):
    """
    Delete S3 objects.

    :param objects: objects to delete as 's3://bucket/object' or 'bucket/object'

    :return: list of objects that failed to delete
    """
    logger = ProcessLog("delete_objects", object_count=len(objects))

    thread_workers = thread_cpus()
    if len(objects) > S3_POOL_COUNT:
        thread_workers = S3_POOL_COUNT

    failed_delete = []
    with ThreadPoolExecutor(max_workers=thread_workers) as pool:
        for result in pool.map(delete_object, objects):
            if isinstance(result, str):
                failed_delete.append(result)

    logger.add_metadata(failed_count=len(failed_delete))
    if len(failed_delete) == 0:
        logger.complete()
    else:
        exception = FileExistsError(f"Failed to delete S3 objects: ({','.join(failed_delete)})")
        logger.failed(exception=exception)

    return failed_delete


def rename_object(from_object: str, to_object: str) -> Optional[str]:
    """
    Rename an S3 object as copy and delete operation.

    :param from_object: COPY from as 's3://bucket/object' or 'bucket/object'
    :param to_object: COPY to as 's3://bucket/object' or 'bucket/object'

    :return: 'from_object' if failed, else None
    """
    logger = ProcessLog("rename_object", from_object=from_object, to_object=to_object)
    try:
        client = get_client()
        copy_source = os.path.join(*split_object(from_object))
        to_bucket, to_key = split_object(to_object)

        client.copy_object(
            Bucket=to_bucket,
            CopySource=copy_source,
            Key=to_key,
        )
        if delete_object(copy_source) is not None:
            raise FileExistsError(f"failed to delete {from_object}")

        logger.complete()
        return None

    except Exception as exception:
        logger.failed(exception)

    return from_object


def _thread_rename_object(args: Tuple[str, str]) -> Optional[str]:
    """Threaded version of rename_object."""
    from_object, to_object = args
    return rename_object(from_object, to_object)


def rename_objects(
    objects: List[str],
    to_bucket: str,
    prepend_prefix: Optional[str] = None,
    replace_prefix: Optional[str] = None,
):
    """
    Rename S3 objects as copy and delete operation.

    'prepend_prefix' and 'replace_prefix' can not be used together,if both are provided,
    'prepend_prefix' will be used and 'replace_prefix' ignored

    :param objects: objects to copy as 's3://bucket/object' or 'bucket/object'
    :param to_bucket: destination bucket as 's3://bucket' or 'bucket'
    :param prepend_prefix: (Optional) prefix to be added to the beginning of all renamed objects
    :param replace_prefix: (Optional) new prefix for all renamed objects

    :return: list of objects that failed to move
    """
    logger = ProcessLog(
        "rename_objects",
        object_count=len(objects),
        to_bucket=to_bucket,
        prepend_prefix=prepend_prefix,
        replace_prefix=replace_prefix,
    )

    failed_rename = []
    to_bucket = to_bucket.replace("s3://", "").split("/", 1)[0]

    thread_objects = []
    for obj in objects:
        _, from_prefix = split_object(obj)
        to_object = os.path.join(to_bucket, from_prefix)
        if prepend_prefix is not None:
            prepend_prefix = prepend_prefix.strip("/")
            to_object = os.path.join(to_bucket, prepend_prefix, from_prefix)
        elif replace_prefix is not None:
            replace_prefix = replace_prefix.strip("/")
            to_object = os.path.join(to_bucket, replace_prefix, os.path.basename(from_prefix))
        thread_objects.append((obj, to_object))

    thread_workers = thread_cpus()
    if len(objects) > S3_POOL_COUNT:
        thread_workers = S3_POOL_COUNT

    with ThreadPoolExecutor(max_workers=thread_workers) as pool:
        for result in pool.map(_thread_rename_object, thread_objects):
            if isinstance(result, str):
                failed_rename.append(result)

    logger.add_metadata(failed_count=len(failed_rename))
    if len(failed_rename) == 0:
        logger.complete()
    else:
        exception = FileExistsError(f"Failed to rename S3 objects: ({','.join(failed_rename)})")
        logger.failed(exception=exception)

    return failed_rename
