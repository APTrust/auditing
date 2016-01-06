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

def record_exists(conn, etag, bucket_name, key, s3_file_last_modified):
    """
    Returns true if the ingest record exists.
    """
    # run exists query by natural key
    pass

def insert_record(conn, data):
    """
    Adds a record to the database. Returns 0 if the record already exists,
    1 if the record was inserted, and -1 if the insert transaction failed.
    """
    if record_exists():
        return 0
    try:
        conn.execute("begin")
        insert_ingest_record(conn, data)
        insert_fetch_result(conn, data)
        insert_tar_result(conn, data)
        insert_unpacked_files(conn, data)
        insert_generic_files(conn, data)
        insert_bag_read_result(conn, data)
        insert_bag_read_files(conn, data)
        insert_checksum_errors(conn, data)
        insert_tags(conn, data)
        insert_fedora_result(conn, data)
        insert_fedora_generic_files(conn, data)
        insert_fedora_metadata(conn, data)
        conn.execute("commit")
        return 1
    except conn.Error:
        print("Insert failed")
        conn.execute("rollback")
        return -1

def insert_ingest_record(conn, data):
    pass

def insert_fetch_result(conn, data):
    pass

def insert_tar_result(conn, data):
    pass

def insert_unpacked_files(conn, data):
    pass

def insert_generic_files(conn, data):
    pass

def insert_bag_read_result(conn, data):
    pass

def insert_bag_read_files(conn, data):
    pass

def insert_checksum_errors(conn, data):
    pass

def insert_tags(conn, data):
    pass

def insert_fedora_result(conn, data):
    pass

def insert_fedora_generic_files(conn, data):
    pass

def insert_fedora_metadata(conn, data):
    pass

def initialize_db(conn):
    """
    Creates the database tables and indexes if they don't already exist.
    """
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
        print("Creating index ix_etag_bucket_key_date on ingest_records")
        statement = """create index ix_etag_bucket_key_date on
        ingest_records(etag, bucket_name, key, s3_file_last_modified)"""
        conn.execute(statement)
        conn.commit()

        # Index for easy name lookup
        print("Creating index ix_key on ingest_records")
        statement = """create index ix_key on
        ingest_records(key)"""
        conn.execute(statement)
        conn.commit()

        # Foreign key indexes
        print("Creating index ix_ingest_fetch_results_fk1")
        statement = """create index ix_ingest_fetch_results_fk1 on
        ingest_fetch_results(ingest_record_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_tar_results_fk1")
        statement = """create index ix_ingest_tar_results_fk1 on
        ingest_tar_results(ingest_record_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_unpacked_files_fk1")
        statement = """create index ix_ingest_unpacked_files_fk1 on
        ingest_unpacked_files(ingest_tar_result_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_generic_files_fk1")
        statement = """create index ix_ingest_generic_files_fk1 on
        ingest_generic_files(ingest_tar_result_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_bag_read_results_fk1")
        statement = """create index ix_ingest_bag_read_results_fk1 on
        ingest_bag_read_results(ingest_record_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_bag_read_files_fk1")
        statement = """create index ix_ingest_bag_read_files_fk1 on
        ingest_bag_read_files(ingest_bag_read_result_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_checksum_errors_fk1")
        statement = """create index ix_ingest_checksum_errors_fk1 on
        ingest_checksum_errors(ingest_bag_read_result_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_ingest_tags_fk1")
        statement = """create index ix_ingest_tags_fk1 on
        ingest_tags(ingest_bag_read_result_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_fedora_results_fk1")
        statement = """create index ix_fedora_results_fk1 on
        ingest_fedora_results(ingest_record_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_fedora_generic_files_fk1")
        statement = """create index ix_fedora_generic_files_fk1 on
        ingest_fedora_generic_files(ingest_fedora_result_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_fedora_metadata_fk1")
        statement = """create index ix_fedora_metadata_fk1 on
        ingest_fedora_metadata(ingest_fedora_result_id)"""
        conn.execute(statement)
        conn.commit()

    c.close()

if __name__ == "__main__":
    if not os.path.exists('db'):
        os.mkdir('db')
    conn = sqlite3.connect('db/aptrust_logs.db')
    # Turn OFF automatic transactions, because we want to
    # manage these manually.
    conn.isolation_level = None
    initialize_db(conn)
    conn.close()
