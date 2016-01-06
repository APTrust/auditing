#! /usr/bin/env python
# json_to_sql.py
"""
Imports JSON data from the apt_record.json log into a SQLite DB.
Specifically, this imports intellectual object and generic file data.
"""
import datetime
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
        ingest_record_id = insert_ingest_record(conn, data)
        ingest_s3_file_id = insert_ingest_s3_file(conn, data, ingest_record_id)
        # insert_fetch_result(conn, data)
        # insert_tar_result(conn, data)
        # insert_unpacked_files(conn, data)
        # insert_generic_files(conn, data)
        # insert_bag_read_result(conn, data)
        # insert_bag_read_files(conn, data)
        # insert_checksum_errors(conn, data)
        # insert_tags(conn, data)
        # insert_fedora_result(conn, data)
        # insert_fedora_generic_files(conn, data)
        # insert_fedora_metadata(conn, data)
        conn.execute("commit")
        return 1
    except sqlite3.Error as err:
        print("Insert failed")
        conn.execute("rollback")
        return -1

def do_insert(conn, statement, values):
    try:
        conn.execute(statement, *values)
        lastrow_id = conn.lastrowid
    except sqlite3.Error as err:
        print(err)
        print(statement, values)
        raise err
    return lastrow_id

def insert_ingest_record(conn, data):
    statement = """
    insert into ingest_records(
      error_message,
      stage,
      retry,
      object_identifier,
      created_at,
      updated_at)
    values(?,?,?,?,?,?)
    """
    lastrow_id = -1
    now = datetime.utcnow()
    object_identifier = get_object_identifier(
        data['S3File']['BucketName'],
        data['S3File']['Key']['Key'])
    values = (data['ErrorMessage'],
              data['Stage'],
              data['Retry'],
              object_identifier,
              now,
              now)
    return do_insert(conn, statement, values)

def insert_s3_file(conn, data, ingest_record_id):
    statement = """
    insert into ingest_s3_files(
      ingest_record_id,
      bucket_name,
      key,
      size,
      etag,
      last_modified,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?,?,?,?,?,?)
    """
    now = datetime.utcnow()
    values = (ingest_record_id,
              data['S3File']['BucketName'],
              data['S3File']['Key']['Key'],
              data['S3File']['Key']['Size'],
              data['S3File']['Key']['ETag'],
              data['S3File']['Key']['LastModified'],
              now,
              now)
    return do_insert(conn, statement, values)

def insert_fetch_result(conn, data):
    statement = """
    insert into ingest_fetch_results(
      ingest_record_id,
      local_file,
      remote_md5,
      local_md5,
      md5_verified,
      md5_verifiable,
      error_message,
      warning,
      retry,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?,?,?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_tar_result(conn, data):
    statement = """
    insert into ingest_tar_results(
      ingest_record_id,
      input_file,
      output_dir,
      error_message,
      warnings,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_unpacked_files(conn, data):
    statement = """
    insert into ingest_unpacked_files(
      ingest_tar_result_id,
      file_path,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_generic_files(conn, data):
    statement = """
    insert into ingest_generic_files(
      ingest_tar_result_id,
      file_path,
      size,
      file_created,
      file_modified,
      md5,
      md5_verified,
      sha256,
      sha256_generated,
      uuid,
      uuid_generated,
      mime_type,
      error_message,
      storage_url,
      stored_at,
      storage_md5,
      identifier,
      existing_file,
      needs_save,
      replication_error,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_bag_read_result(conn, data):
    statement = """
    insert into ingest_bag_read_results(
      ingest_record_id,
      bag_path,
      error_message,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_bag_read_files(conn, data):
    statement = """
    insert into ingest_bag_read_files(
      ingest_bag_read_result_id,
      file_path,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_checksum_errors(conn, data):
    statement = """
    insert into ingest_checksum_errors(
      ingest_bag_read_result_id,
      error_message,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_tags(conn, data):
    statement = """
    insert into ingest_tags(
      ingest_bag_read_result_id,
      label text,
      value text,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_fedora_result(conn, data):
    statement = """
    insert into ingest_fedora_results(
      ingest_record_id int not null,
      object_identifier text,
      is_new_object bool,
      error_message text,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_fedora_generic_files(conn, data):
    statement = """
    insert into ingest_fedora_results(
      ingest_fedora_result_id,
      file_path,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

def insert_fedora_metadata(conn, data):
    statement = """
    insert into ingest_fedora_metadata(
      ingest_fedora_result_id,
      record_type,
      action,
      event_object,
      error_message,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?,?)
    """
    values = ()
    return do_insert(conn, statement, values)

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
        object_identifier text,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp)"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_s3_files")
        statement = """create table ingest_s3_files(
        id integer primary key autoincrement,
        ingest_record_id int not null,
        bucket_name text,
        key text,
        size int,
        etag text,
        last_modified datetime,
        created_at datetime default current_timestamp,
        updated_at datetime default current_timestamp,
        FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table ingest_fetch_results")
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
        print("Creating index ix_etag_bucket_key_date on ingest_s3_files")
        statement = """create index ix_etag_bucket_key_date on
        ingest_s3_files(etag, bucket_name, key, last_modified)"""
        conn.execute(statement)
        conn.commit()

        # Index for easy tar file name lookup
        print("Creating index ix_key on ingest_s3_files")
        statement = """create index ix_key on ingest_s3_files(key)"""
        conn.execute(statement)
        conn.commit()

        # Index for easy object identifier lookup
        print("Creating index ix_obj_identifier on ingest_records")
        statement = """create index ix_obj_identifier on
        ingest_records(object_identifier)"""
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
