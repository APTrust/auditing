#! /usr/bin/env python
#
# cleanup_001.py
#
"""
Deletes some duplicate files from S3 and copies
some files from S3 to Glacier that had not previously
been coped to Glacier.
"""

import os
import sqlite3
import sys
from boto.s3.connection import S3Connection
from datetime import datetime

VA_BUCKET_NAME = "aptrust.preservation.storage"
OR_BUCKET_NAME = "aptrust.preservation.oregon"

S3_PREFIX = "https://s3.amazonaws.com/aptrust.preservation.storage/"
GLACIER_PREFIX = "https://s3.amazonaws.com/aptrust.preservation.oregon/"

def copy_missing_files_to_glacier(conn, va_bucket, or_bucket):
    """
    Copy files from S3 bucket in Virginia to Glacier in Oregon.
    We're working on a list of items we know are in S3 but not
    yet in Glacier.
    """
    c = conn.cursor()
    query = """select id, key from aws_files where action = 'add'
    and action_completed_at is null"""
    c.execute(query)
    for row in c.fetchall():
        pk = row[0]
        uuid = row[1]
        s3_url = S3_PREFIX + uuid
        glacier_url = GLACIER_PREFIX + uuid
        sys.stderr.write("Copying {0} to {1}\n".format(uuid, glacier_url))
        copy_file(va_bucket, or_bucket, uuid)
        mark_as_completed(conn, pk)
    c.close()

def copy_file(va_bucket, or_bucket, uuid):
    """
    This performs the remote copy.
    """
    key = va_bucket.get_key(uuid)
    metadata = key.metadata
    header_data = {"Content-Type": key.content_type }
    print("    {0} to OR".format(uuid))
    or_bucket.copy_key(uuid, VA_BUCKET_NAME, uuid, headers=header_data, metadata=metadata)

def mark_as_completed(conn, pk):
    """
    Put a timestamp in the database, so we can create a PREMIS event saying
    when the add/remove action was completed.
    """
    cursor = conn.cursor()
    statement = "update aws_files set action_completed_at=? where id=?"
    now = datetime.utcnow()
    values = (now, pk,)
    print("    {0}").format(now.isoformat())
    cursor.execute(statement, values)
    conn.commit()
    cursor.close()

def delete_duplicate_files(conn, va_bucket):
    """
    For files that were ingested twice, we want to delete one of the duplicates.
    The duplicates are in S3 only. There are no duplicates in Glacier.
    """
    c = conn.cursor()
    query = """select id, key from aws_files where action = 'delete'
    and action_completed_at is null"""
    c.execute(query)
    for row in c.fetchall():
        pk = row[0]
        uuid = row[1]
        s3_url = S3_PREFIX + uuid
        sys.stderr.write("Removing {0}\n".format(uuid))
        delete_file(va_bucket, uuid)
        mark_as_completed(conn, pk)
    c.close()

def delete_file(va_bucket, uuid):
    """
    Deletes a file from our S3 bucket in Virginia.
    """
    key = va_bucket.get_key(uuid)
    print("    {0} deleted from VA".format(uuid))
    va_bucket.delete_key(uuid)

if __name__ == "__main__":
    s3 = S3Connection()
    va_bucket = s3.get_bucket(VA_BUCKET_NAME)
    or_bucket = s3.get_bucket(OR_BUCKET_NAME)
    conn = sqlite3.connect('db/audit001_summary.db')
    print("--- Copying to Glacier ---")
    copy_missing_files_to_glacier(conn, va_bucket, or_bucket)
    print("--- Deleting duplicates from S3 ---")
    delete_duplicate_files(conn, va_bucket)
    conn.close()
