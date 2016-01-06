#! /usr/bin/env python
# json_to_sql.py
"""
Imports JSON data from the apt_record.json log into a SQLite DB.
Specifically, this imports intellectual object and generic file data.
"""
import os
import sqlite3
import sys

# http://stackoverflow.com/questions/15856976/transactions-with-python-sqlite3

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
        object_identifier text,
        size int,
        etag text,
        s3_file_last_modified datetime,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp)"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_fetch_result")
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
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_tar_results")
        statement = """create table ingest_tar_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        input_file text,
        output_dir text,
        error_message text,
        warnings text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_unpacked_files")
        statement = """create table ingest_unpacked_files(
        id integer primary key autoincrement,
        ingest_tar_result_id int not null,
        file_path text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_tar_result_id)
        REFERENCES ingest_tar_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_generic_files")
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
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id),
        FOREIGN KEY(ingest_tar_result_id)
        REFERENCES ingest_tar_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_bag_read_results")
        statement = """create table ingest_bag_read_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        bag_path text,
        error_message text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_bag_read_files")
        statement = """create table ingest_bag_read_files(
        id integer primary key autoincrement,
        ingest_bag_read_result_id int not null,
        file_path text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_bag_read_result_id)
        REFERENCES ingest_bag_read_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_checksum_errors")
        statement = """create table ingest_checksum_errors(
        id integer primary key autoincrement,
        ingest_bag_read_result_id int not null,
        error_message text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_bag_read_result_id)
        REFERENCES ingest_bag_read_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_tags")
        statement = """create table ingest_tags(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        ingest_bag_read_result_id int not null,
        label text,
        value text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id),
        FOREIGN KEY(ingest_bag_read_result_id)
        REFERENCES ingest_bag_read_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_fedora_results")
        statement = """create table ingest_fedora_results(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        object_identifier text,
        is_new_object bool,
        error_message text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_fedora_generic_files")
        statement = """create table ingest_fedora_generic_files(
        id integer primary key autoincrement,
        ingest_fedora_result_id int not null,
        file_path text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_fedora_result_id)
        REFERENCES ingest_fedora_results(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_fedora_metadata")
        statement = """create table ingest_fedora_metadata(
        id integer primary key autoincrement,
        ingest_fedora_result_id int not null,
        record_type text,
        action text,
        event_object string,
        error_message string,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_fedora_result_id)
        REFERENCES ingest_fedora_results(id))"""
        conn.execute(statement)
        conn.commit()

        # Natural key for items in receiving buckets
        print("Creating index ix_etag_bucket_key on ingest_records")
        statement = """create index ix_etag_bucket_key on
        ingest_records(etag, bucket_name, key)"""
        conn.execute(statement)
        conn.commit()

        # Index for easy name lookup
        print("Creating index ix_key on ingest_records")
        statement = """create index ix_key on
        ingest_records(key)"""
        conn.execute(statement)
        conn.commit()

    c.close()

if __name__ == "__main__":
    if not os.path.exists('db'):
        os.mkdir('db')
    conn = sqlite3.connect('db/aptrust_logs.db')
    ensure_tables_exist(conn)
    conn.close()
