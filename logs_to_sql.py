#! /usr/bin/env python
# json_to_sql.py
"""
Imports JSON data from the apt_record.json log into a SQLite DB.
Specifically, this imports intellectual object and generic file data.
"""
from datetime import datetime
import json
import os
import sqlite3
import sys

# http://stackoverflow.com/questions/15856976/transactions-with-python-sqlite3

def import_json(file_path, conn):
    line_number = 0
    records_inserted = 0
    with open(file_path) as f:
        for line in f:
            line_number += 1
            if line_number % 500 == 0:
                print("Processed {0} lines".format(line_number))
            try:
                data = json.loads(line)
            except ValueError as err:
                print("Error decoding JSON on line {0}: {1}".format(line_number, err))
            records_inserted += insert_record(conn, data)
    print("Processed {0} json records. Inserted {1} new records".format(
        line_number, records_inserted))

def get_object_identifier(bucket_name, key):
    institution = bucket_name.replace('aptrust.receiving.', '', 1)
    return "{0}/{1}".format(institution, key)

def record_exists(conn, etag, bucket_name, key, s3_file_last_modified):
    """
    Returns true if the ingest record exists.
    """
    # this natural key is indexed
    statement = """
    select exists(select 1 from ingest_s3_files where
    etag=? and bucket_name=? and key=? and last_modified=?)
    """
    values = (etag, bucket_name, key, s3_file_last_modified)
    cursor = conn.cursor()
    cursor.execute(statement, values)
    result = cursor.fetchone()
    cursor.close()
    return result[0] == 1

def insert_record(conn, data):
    """
    Adds a record to the database. Returns 0 if the record already exists,
    1 if the record was inserted, and -1 if the insert transaction failed.
    """
    if record_exists(conn,
                     data['S3File']['Key']['ETag'].replace('"', ''),
                     data['S3File']['BucketName'],
                     data['S3File']['Key']['Key'],
                     data['S3File']['Key']['LastModified']):
        return 0
    try:
        conn.execute("begin")
        ingest_record_id = insert_ingest_record(conn, data)

        if data['FetchResult'] is not None:
            ingest_s3_file_id = insert_s3_file(conn, data, ingest_record_id)
            insert_fetch_result(conn, data, ingest_record_id)

        if data['TarResult'] is not None:
            tar_result_id = insert_tar_result(conn, data, ingest_record_id)

            if data['TarResult']['FilesUnpacked'] is not None:
                for file_path in data['TarResult']['FilesUnpacked']:
                    insert_unpacked_files(conn, file_path, tar_result_id)

            if data['TarResult']['Files'] is not None:
                for generic_file in data['TarResult']['Files']:
                    insert_generic_files(conn, generic_file, tar_result_id)

        if data['BagReadResult'] is not None:
            bag_read_result_id = insert_bag_read_result(
                conn, data, ingest_record_id)

            if data['BagReadResult']['Files'] is not None:
                for file_path in data['BagReadResult']['Files']:
                    insert_bag_read_files(conn, file_path, bag_read_result_id)

            if data['BagReadResult']['ChecksumErrors'] is not None:
                for checksum_error in data['BagReadResult']['ChecksumErrors']:
                    insert_checksum_errors(conn, checksum_error, bag_read_result_id)

            if data['BagReadResult']['Tags'] is not None:
                for tag in data['BagReadResult']['Tags']:
                    insert_tags(conn, tag, bag_read_result_id)

        if data['FedoraResult'] is not None:
            fedora_result_id = insert_fedora_result(conn, data, ingest_record_id)

            if data['FedoraResult']['GenericFilePaths'] is not None:
                for file_path in data['FedoraResult']['GenericFilePaths']:
                    insert_fedora_generic_files(conn, file_path, fedora_result_id)

            if data['FedoraResult']['MetadataRecords'] is not None:
                for metadata_obj in data['FedoraResult']['MetadataRecords']:
                    insert_fedora_metadata(conn, metadata_obj, fedora_result_id)

        conn.execute("commit")
        return 1

    except sqlite3.Error as err:
        print("Insert failed for record {0}/{1}".format(
            data['S3File']['BucketName'],
            data['S3File']['Key']['Key']))
        conn.execute("rollback")
        return -1

def do_insert(conn, statement, values):
    try:
        cursor = conn.cursor()
        cursor.execute(statement, values)
        lastrow_id = cursor.lastrowid
    except sqlite3.Error as err:
        print(err)
        print(statement, values)
        raise err
    finally:
        cursor.close()
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
    values(?,?,?,?,?,?,?,?)
    """
    now = datetime.utcnow()
    values = (ingest_record_id,
              data['S3File']['BucketName'],
              data['S3File']['Key']['Key'],
              data['S3File']['Key']['Size'],
              data['S3File']['Key']['ETag'].replace('"', ''),
              data['S3File']['Key']['LastModified'],
              now,
              now)
    return do_insert(conn, statement, values)

def insert_fetch_result(conn, data, ingest_record_id):
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
    now = datetime.utcnow()
    values = (ingest_record_id,
              data['FetchResult']['LocalFile'],
              data['FetchResult']['RemoteMd5'],
              data['FetchResult']['LocalMd5'],
              data['FetchResult']['Md5Verified'],
              data['FetchResult']['Md5Verifiable'],
              data['FetchResult']['ErrorMessage'],
              data['FetchResult']['Warning'],
              data['FetchResult']['Retry'],
              now,
              now,
    )
    return do_insert(conn, statement, values)

def insert_tar_result(conn, data, ingest_record_id):
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
    now = datetime.utcnow()
    values = (ingest_record_id,
              data['TarResult']['InputFile'],
              data['TarResult']['OutputDir'],
              data['TarResult']['ErrorMessage'],
              data['TarResult']['Warnings'],
              now,
              now,
    )
    return do_insert(conn, statement, values)

def insert_unpacked_files(conn, file_path, tar_result_id):
    statement = """
    insert into ingest_unpacked_files(
      ingest_tar_result_id,
      file_path,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    now = datetime.utcnow()
    values = (tar_result_id, file_path, now, now)
    return do_insert(conn, statement, values)

def insert_generic_files(conn, generic_file, tar_result_id):
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
      identifier_assigned,
      existing_file,
      needs_save,
      replication_error,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    now = datetime.utcnow()
    values = (tar_result_id,
              generic_file['Path'],
              generic_file['Size'],
              generic_file['Created'],
              generic_file['Modified'],
              generic_file['Md5'],
              generic_file['Md5Verified'],
              generic_file['Sha256'],
              generic_file['Sha256Generated'],
              generic_file['Uuid'],
              generic_file['UuidGenerated'],
              generic_file['MimeType'],
              generic_file['ErrorMessage'],
              generic_file['StorageURL'],
              generic_file['StoredAt'],
              generic_file['StorageMd5'],
              generic_file['Identifier'],
              generic_file['IdentifierAssigned'],
              generic_file['ExistingFile'],
              generic_file['NeedsSave'],
              generic_file['ReplicationError'],
              now,
              now,
    )
    return do_insert(conn, statement, values)

def insert_bag_read_result(conn, data, ingest_record_id):
    statement = """
    insert into ingest_bag_read_results(
      ingest_record_id,
      bag_path,
      error_message,
      created_at,
      updated_at
    )
    values(?,?,?,?,?)
    """
    now = datetime.utcnow()
    values = (ingest_record_id,
              data['BagReadResult']['Path'],
              data['BagReadResult']['ErrorMessage'],
              now,
              now,
          )
    return do_insert(conn, statement, values)

def insert_bag_read_files(conn, file_path, bag_read_result_id):
    statement = """
    insert into ingest_bag_read_files(
      ingest_bag_read_result_id,
      file_path,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    now = datetime.utcnow()
    values = (bag_read_result_id, file_path, now, now)
    return do_insert(conn, statement, values)

def insert_checksum_errors(conn, checksum_error, bag_read_result_id):
    statement = """
    insert into ingest_checksum_errors(
      ingest_bag_read_result_id,
      error_message,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    now = datetime.utcnow()
    values = (bag_read_result_id,
              checksum_error,
              now,
              now)
    return do_insert(conn, statement, values)

def insert_tags(conn, data, bag_read_result_id):
    statement = """
    insert into ingest_tags(
      ingest_bag_read_result_id,
      label,
      value,
      created_at,
      updated_at
    )
    values(?,?,?,?,?)
    """
    now = datetime.utcnow()
    values = (bag_read_result_id,
              data['Label'],
              data['Value'],
              now,
              now)
    return do_insert(conn, statement, values)

def insert_fedora_result(conn, data, ingest_record_id):
    statement = """
    insert into ingest_fedora_results(
      ingest_record_id,
      object_identifier,
      is_new_object,
      error_message,
      created_at,
      updated_at
    )
    values(?,?,?,?,?,?)
    """
    now = datetime.utcnow()
    values = (ingest_record_id,
              data['FedoraResult']['ObjectIdentifier'],
              data['FedoraResult']['IsNewObject'],
              data['FedoraResult']['ErrorMessage'],
              now,
              now)
    return do_insert(conn, statement, values)

def insert_fedora_generic_files(conn, file_path, fedora_result_id):
    statement = """
    insert into ingest_fedora_generic_files(
      ingest_fedora_result_id,
      file_path,
      created_at,
      updated_at
    )
    values(?,?,?,?)
    """
    now = datetime.utcnow()
    values = (fedora_result_id,
              file_path,
              now,
              now)
    return do_insert(conn, statement, values)

def insert_fedora_metadata(conn, metadata_obj, fedora_result_id):
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
    now = datetime.utcnow(fedora_result_id,
                          metadata_obj['Type'],
                          metadata_obj['Action'],
                          metadata_obj['EventObject'],
                          metadata_obj['ErrorMessage'],
                          now,
                          now)
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
        identifier_assigned datetime,
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
    if len(sys.argv) < 2:
        print("Missing arg: path to json log file")
        print("Usage: python logs_to_sql.py <path/to/logfile.json>")
        sys.exit()
    if not os.path.exists('db'):
        os.mkdir('db')
    conn = sqlite3.connect('db/aptrust_logs.db')
    # Turn OFF automatic transactions, because we want to
    # manage these manually.
    conn.isolation_level = None
    initialize_db(conn)
    import_json(sys.argv[1], conn)
    conn.close()
