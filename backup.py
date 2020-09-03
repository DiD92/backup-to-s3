#!/bin/python3
import argparse
import smtplib
import ssl
import syslog
from datetime import datetime
from os import environ as env, remove as rm
from os.path import abspath, basename, isdir, sep as pathsep, split
from shutil import make_archive, rmtree
from tempfile import mkdtemp

import boto3
from boto3.exceptions import S3UploadFailedError
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
        syslog.syslog(syslog.LOG_INFO,
                      f'Found valid S3 Bucket - {bucket_name}')
        return s3.Bucket(bucket_name)
    except ClientError as e:
        syslog.syslog(syslog.LOG_ERR,
                      f'Invalid S3 Bucket - {bucket_name} - {e}')
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
        syslog.syslog(syslog.LOG_INFO,
                      f'Uploaded {file_path} to S3 Bucket - {bucket.name}')
        return True
    except S3UploadFailedError as s3ex:
        syslog.syslog(
            syslog.LOG_ERR, f'Failed to upload {file_path} to S3 Bucket - {bucket_name} - {s3ex}')
        return False
    finally:
        rm(file_path)


def send_email(content, recipients):
    body_content = '\n'.join(content)
    email_body = f'Subject: Backup system info\nFolders backed up:\n{body_content}'

    syslog.syslog(syslog.LOG_INFO, f'Sending email/s to {recipients}')

    port = env.get('BACKUP_SYSTEM_SMTP_PORT', 465)
    passwd = env.get('BACKUP_SYSTEM_SMTP_PASS', None)
    sender_email = env.get('BACKUP_SYSTEM_SMTP_EMAIL', None)
    smtp_server = env.get('BACKUP_SYSTEM_SMTP_ADDR', 'smtp.gmail.com')

    if not passwd or not send_email:
        syslog.syslog(syslog.LOG_WARNING, f'No valid login credentials for SMTP found!')
        return

    try:
        context = ssl.create_default_context()

        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, passwd)

            for recipient in recipients:
                server.sendmail(sender_email, recipient, email_body)
    except smtplib.SMTPException as smtpex:
        syslog.syslog(syslog.LOG_ERR, f'Error delivering email!: {smtpex}')


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
    parser.add_argument('-e', '--recipients',
                        help='Comma separated list of email recipients')

    parsed_args, _ = parser.parse_known_args()

    bucket_name = parsed_args.bucket
    prefix = parsed_args.prefix
    folder_list = parsed_args.folder
    add_timestamp = parsed_args.timestamp
    email_list = parsed_args.recipients.split(',')

    # Bucket validation
    if bucket := is_valid_bucket(bucket_name):
        # Folder filtering
        folder_list = [abspath(f) for f in folder_list if isdir(abspath(f))]

        if not folder_list:
            syslog.syslog(syslog.LOG_ERR,
                          f'No valid folder list to upload to S3')
            raise ValueError('None of the supplied folder paths are valid!')

        base_tmp_dir = mkdtemp()

        timestamp = f'{datetime.now().strftime("%Y%m%d%H%M%S")}_' if add_timestamp else None

        if email_list:
            upload_results = []

        for folder in folder_list:
            _, folder_name = split(folder)

            out_path = f'{base_tmp_dir}{pathsep}{folder_name}'
            path_to_upload = make_archive(
                base_dir='.', root_dir=folder, format='zip', base_name=out_path)

            upload_success = upload_to_s3(
                bucket, path_to_upload, prefix, timestamp)

            if email_list:
                upload_results.append(
                    f'Uploaded {path_to_upload} to {bucket.name}')

        rmtree(base_tmp_dir)

        if upload_success and email_list:
            send_email(upload_results, email_list)
    else:
        raise ValueError(f'Invalid target bucket {bucket_name}!')
