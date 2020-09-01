#!/bin/python3
import argparse
from os.path import abspath, basename, isdir, sep as pathsep, split
from tempfile import mkdtemp
from shutil import make_archive, rmtree

import boto3
from botocore.exceptions import ClientError


def is_valid_bucket(bucket_name: str):
    """Checks if the bucket both exists and that we can write to it.

    Args:
        bucket_name (str): Bucket to check

    Returns:
        Optional[s3.Bucket]: The Bucket with 'bucket_name' if it exists and
            is writtable, None otherwise.
    """

    s3 = boto3.resource('s3')

    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return s3.Bucket(bucket_name)
    except ClientError as e:
        return None


def upload_to_s3(bucket, file_path, prefix, timestamp):
    """Uploads a file to S3 adding the corresponding prefixes and
    timestamp suffix if necessary.

    Args:
        bucket (S3.Bucket): Bucket to which the file will be uploaded
        file_path (str): Path in which the file to upload is found
        prefix (str): Prefix to add to the uploaded file name
        timestamp (Optional[str]): Timestamp mark to add to the file name

    Returns:
        [type]: [description]
    """
    upload_name = f'{prefix}_{timestamp or ""}{basename(file_path)}'

    try:
        bucket.upload_file(file_path, upload_name)
        return True
    except boto3.exceptions.S3UploadFailedError:
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Backs up the given list of folders to the target S3 bucket')

    parser.add_argument('-b', '--bucket', required=True,
                        help='Target S3 bucket to which back up the folder')
    parser.add_argument('-p', '--prefix',
                        help='Prefix for file uploads')
    parser.add_argument('folder', nargs='+',
                        help='Folder to back up')
    parser.add_argument('-ts', '--timestamp',
                        help='Add timestamp suffix to the uploaded files name?',
                        action='store_true')
    parser.add_argument('-e', '--recipients', nargs='*',
                        help='Send email on process completed to the listed recipients')

    parsed_args, _ = parser.parse_known_args()

    bucket_name = parsed_args.bucket
    prefix = parsed_args.prefix
    folder_list = parsed_args.folder
    add_timestamp = parsed_args.timestamp
    email_list = parsed_args.recipients

    # Bucket validation
    if bucket := is_valid_bucket(bucket_name):
        # Folder filtering
        folder_list = [abspath(f) for f in folder_list if isdir(abspath(f))]

        if not folder_list:
            raise ValueError('None of the supplied folder paths are valid!')

        base_tmp_dir = mkdtemp()

        timestamp = f'{datetime.now().strftime("%Y%m%d%H%M%S")}_' if add_timestamp else None

        for folder in folder_list:
            _, folder_name = split(folder)

            out_path = f'{base_tmp_dir}{pathsep}{folder_name}'
            path_to_upload = make_archive(base_dir='.', root_dir=folder, format='zip', base_name=out_path)

            upload_result = upload_to_s3(bucket, path_to_upload, prefix, timestamp)

        # TODO: For each valid folder we will compress it in a temp location,
        # add a timestamp if provided and upload to S3, if any errors
        # occur during this process log them through syslog, or send
        # an error email.
    else:
        raise ValueError(f'Invalid target bucket {bucket_name}!')
