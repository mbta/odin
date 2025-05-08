"""
Util functions for calling s3.

All functions in this file can take paths that start with s3://,
or paths to a local filesystem.
"""

from contextlib import contextmanager
import gzip
import os
from typing import List

import odin.utils.aws.s3 as s3


@contextmanager
def open_file(path: str):
    """
    Context manager that yields a file-like object (with a .read() method)

    Path may start with s3:// or be a local filesystem file.
    If the path ends with .gz, unzips it.

    Throws FileNotFoundError if opening locally, or NoSuchKey if opening from s3.
    """
    if path.startswith("s3://"):
        # technically this is a StreamingBody, not a file
        # but it has a .read() method which makes it file-like enough for the next steps
        s3_stream = s3.stream_object(path)
        if path.endswith(".gz"):
            with gzip.GzipFile(fileobj=s3_stream) as f:
                yield f
        else:
            yield s3_stream
    else:
        if path.endswith(".gz"):
            with gzip.open(path, "r") as f:
                yield f
        else:
            with open(path, "r") as f:
                yield f


def list_objects(prefix: str) -> List[str]:
    """
    Return list of paths, relative to prefix

    path may start with s3:// or be a local filesystem folder
    """
    if prefix.startswith("s3://"):
        s3_objects = s3.list_objects(prefix)
        return [os.path.relpath(s3_object.path, prefix) for s3_object in s3_objects]
    else:
        paths = []
        for root, _dirs, files in os.walk(prefix):
            for file in files:
                relative_path = os.path.relpath(os.path.join(root, file), prefix)
                paths.append(relative_path)
        return paths


def rename_object(source: str, dest: str) -> None:
    """
    Move / rename an object.

    If both paths start with s3://, then this is done with a copy, then delete.
    If both paths are local, then done as a move.
    Paths must be both in s3 or both local.
    """
    if source.startswith("s3://") and dest.startswith("s3://"):
        s3.rename_object(source, dest)
    elif not source.startswith("s3://") and not dest.startswith("s3://"):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        os.rename(source, dest)
    else:
        raise Exception(f"Can't move between s3 and local files. Got source={source} dest={dest}")
