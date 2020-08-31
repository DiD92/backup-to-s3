#!/bin/python3
import argparse
from os.path import abspath, isdir

import boto3
from botocore.exceptions import ClientError

def is_valid_bucket(bucket_name: str):
    """Checks if the bucket both exists and that we can write to it.

    Args:
        bucket_name (str): Bucket to check

    Returns:
        Optional[s3.Bucket]: True if the bucket named 'bucket_name' exists and 
            can be written to, return False otherwise.
    """

    s3 = boto3.resource('s3')

    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return s3.Bucket(bucket_name)
    except ClientError as e:
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Backs up the given list of folders to the target S3 bucket')

    parser.add_argument('-b', '--bucket', required=True,
                        help='Target S3 bucket to which back up the folder')
    parser.add_argument('folder', nargs='+',
                        help='Folder to back up')
    parser.add_argument('-ts', '--timestamp', 
                        help='Add timestamp suffix to the uploaded files name?',
                        action='store_true')
    # TODO: Add email argument

    parsed_args, _ = parser.parse_known_args()

    bucket_name = parsed_args.bucket
    folder_list = parsed_args.folder
    add_timestamp = parsed_args.timestamp

    # Bucket validation
    if bucket := is_valid_bucket(bucket_name):
        # Folder filtering
        folder_list = [f for f in folder_list if isdir(abspath(f))]

        # TODO: For each valid folder we will compress it in a temp location,
        # add a timestamp if provided and upload to S3, if any errors
        # occur during this process log them through syslog, or send
        # an error email.
    else:
        raise ValueError(f'Invalid target bucket {bucket_name}!')

    print(bucket_name, folder_list, add_timestamp)
