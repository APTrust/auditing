#! /usr/bin/env python
# json_to_sql.py
"""
Imports JSON data from the apt_record.json log into a SQLite DB.
Specifically, this imports intellectual object and generic file data.
"""

import sqlite3
import sys


def ensure_tables_exist(conn):
    query = """SELECT name FROM sqlite_master WHERE type='table'
    AND name='ingest_records'"""
    c = conn.cursor()
    c.execute(query)
    row = c.fetchone()
    if not row or len(row) < 1:
        print("Creating table ingest_records")
        statement = """create table ingest_records(
        id integer primary key autoincrement,
        error_message text,
        stage text,
        retry bool,
        bucket_name text,
        key text,
        size int,
        etag text,
        s3_file_last_modified datetime,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp)"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_fetch_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        local_file text,
        remote_md5 text,
        local_md5 text,
        md5_verified bool,
        md5_verifiable bool,
        error_message text,
        warning text,
        retry bool,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_tar_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        input_file text,
        output_dir text,
        error_message text,
        warnings text,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_unpacked_files(
        id integer primary key autoincrement,
        ingest_tar_result_id int not null,
        file_path text,
        FOREIGN KEY(ingest_tar_result_id)
        REFERENCES ingest_tar_results(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_generic_files(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        ingest_tar_result_id int not null,
        file_path text,
        size int,
        file_created datetime,
        file_modified datetime,
        md5 text,
        md5_verified bool,
        sha256 text,
        sha256_generated datetime,
        uuid text,
        uuid_generated datetime,
        mime_type text,
        error_message text,
        storage_url text,
        stored_at datetime,
        storage_md5 text,
        identifier text,
        existing_file bool,
        needs_save bool,
        replication_error text,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id),
        FOREIGN KEY(ingest_tar_result_id)
        REFERENCES ingest_tar_results(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_bag_read_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        bag_path text,
        error_message text,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_bag_read_files(
        id integer primary key autoincrement,
        ingest_bag_read_result_id int not null,
        file_path text,
        FOREIGN KEY(ingest_bag_read_result_id)
        REFERENCES ingest_bag_read_results(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_bag_read_checksum_errors(
        id integer primary key autoincrement,
        ingest_bag_read_result_id int not null,
        error_message text,
        FOREIGN KEY(ingest_bag_read_result_id)
        REFERENCES ingest_bag_read_results(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_bag_read_tags(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        ingest_bag_read_result_id int not null,
        label text,
        value text,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id),
        FOREIGN KEY(ingest_bag_read_result_id)
        REFERENCES ingest_bag_read_results(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_fedora_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        object_identifier text,
        is_new_object bool,
        error_message text,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_fedora_generic_files(
        id integer primary key autoincrement,
        ingest_fedora_result_id int not null,
        file_path text,
        FOREIGN KEY(ingest_fedora_result_id)
        REFERENCES ingest_fedora_results(id))"""
        conn.execute(statement)
        conn.commit()

        statement = """create table ingest_fedora_metadata(
        id integer primary key autoincrement,
        ingest_fedora_result_id int not null,
        record_type text,
        action text,
        event_object string,
        error_message string,
        FOREIGN KEY(ingest_fedora_result_id)
        REFERENCES ingest_fedora_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_key_etag_bucket on ingest_records")
        statement = """create index ix_key_etag_bucket on
        ingest_records(key, etag, bucket)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_key on ingest_records")
        statement = """create index ix_key on
        ingest_records(key)"""
        conn.execute(statement)
        conn.commit()

    c.close()
