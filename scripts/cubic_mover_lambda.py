from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    """Prototype: move an object from mover_test/incoming/ to mover_test/archive/"""
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = unquote_plus(record["object"]["key"])
    dest_key = key.replace("mover_test/incoming/", "mover_test/archive/", 1)

    s3.copy_object(Bucket=bucket, Key=dest_key, CopySource={"Bucket": bucket, "Key": key})
    s3.delete_object(Bucket=bucket, Key=key)
    return {"message": f"moved s3://{bucket}/{key} to s3://{bucket}/{dest_key}"}
