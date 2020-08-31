#!/bin/python3
import argparse
import os

import boto3


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Backs up the given list of folders to the target S3 bucket')

    parser.add_argument('-b', '--bucket', required=True,
                        help='Target S3 bucket to which back up the folder')
    parser.add_argument('folder', nargs='+',
                        help='Folder to back up')
    parser.add_argument('-c', '--compress',
                        help='Try to compress the folder before uploading?',
                        action='store_true')
    parser.add_argument('-ts', '--timestamp', 
                        help='Add timestamp suffix to the uploaded files name?',
                        action='store_true')

    parsed_args, _ = parser.parse_known_args()

    target_bucket = parsed_args.bucket
    folder_list = parsed_args.folder
    compress_folders = parsed_args.compress
    add_timestamp = parsed_args.timestamp

    print(target_bucket, folder_list, compress_folders, add_timestamp)
