#! /usr/bin/env python
# http_failure_report.py
"""
This script gathers details about bags that appear to have failed ingest
due to bad HTTP responses from Fluctus. The bags appear to have been
either partially or completely ingested, but Fluctus was not able to
confirm the ingest because it returned an HTTP response with an
"invalid byte in chunk length" error, or with a premature EOF.

This script helps us determine which bags were partially ingested, and
which were fully ingested.
"""
import sqlite3

def run_report(conn):
    query = "select * from work_items where stage='Record' and status='Failed'"
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchmany(size=20)
    while rows:
        for row in rows:
            obj_identifier = get_object_identifier(row)
            bag = {}
            bag['obj_identifier'] = obj_identifier
            bag['ingest_files'] = files_read_at_ingest(conn, obj_identifier)
            print_item_report(bag)
        rows = cursor.fetchmany(size=20)
    cursor.close()

def print_item_report(bag):
    print(bag['obj_identifier'])
    print(bag['ingest_files'])

def files_read_at_ingest(conn, object_identifier):
    ids = get_ingest_ids(conn, object_identifier)
    if ids is None:
        print("No ingest record for {0}".format(object_identifier))

    # Get a list of all files unpacked from the tarred bag.
    # Only files under the data directory go to S3/Glacier.
    files_unpacked = get_list(conn,
                              'ingest_unpacked_files',
                              'ingest_tar_result_id',
                              ids['tar_result_id'],
                              'file_path')

    # Get a list of the generic files created during ingest.
    # The list should include only files under the data dir.
    # These are generic files that ingest knows about, but
    # the may not have been successfully recorded in Fluctus.
    # This is the authoritative list of files that *should*
    # go into S3/Glacier.
    generic_file_cols = ('file_path', 'uuid', 'identifier',
                         'storage_url', 'stored_at')
    generic_files = get_hashes(conn,
                               'ingest_generic_files',
                               'ingest_tar_result_id',
                               ids['tar_result_id'],
                               generic_file_cols)
    return { 'files_unpacked': files_unpacked,
             'generic_files': generic_files }

def get_object_identifier(row):
    if row['object_identifier'] is not None:
        return row['object_identifier']
    inst = row['bucket'].replace('aptrust.receiving.', '')
    return "{0}/{1}".format(inst, row['name'])

def get_ingest_ids(conn, object_identifier):
    if object_identifier is None:
        print("Can't look up null object_identifier")
        return
    ingest_record_id = get_id(conn, 'ingest_records', 'object_identifier',
                              object_identifier)
    if ingest_record_id is None:
        return None
    tar_result_id = get_id(conn, 'ingest_tar_results', 'ingest_record_id',
                           ingest_record_id)
    return { 'ingest_record_id': ingest_record_id,
             'tar_result_id': tar_result_id }

def get_id(conn, table, column, value):
    query = "select id from {0} where {1}=?".format(table, column)
    values = (value,)
    cursor = conn.cursor()
    cursor.execute(query, values)
    row = cursor.fetchone()
    row_id = None
    if row:
        row_id = row[0]
    return row_id

def get_list(conn, table, where_column, value, column_to_fetch):
    query = "select {0} from {1} where {2}=?".format(
        column_to_fetch, table, where_column)
    values = (value,)
    cursor = conn.cursor()
    cursor.execute(query, values)
    results = []
    for row in cursor.fetchall():
        results.append(row[0])
    cursor.close()
    return results

def get_hashes(conn, table, where_column, value, columns_to_fetch):
    cols = ", ".join(columns_to_fetch)
    query = "select {0} from {1} where {2}=?".format(
        cols, table, where_column)
    values = (value,)
    cursor = conn.cursor()
    cursor.execute(query, values)
    results = []
    for row in cursor.fetchall():
        data = {}
        for col in columns_to_fetch:
            data[col] = row[col]
        results.append(data)
    cursor.close()
    return results


def files_in_fedora(conn, row):
    pass

def files_in_s3_and_glacier(conn, row):
    pass

if __name__ == "__main__":
    conn = sqlite3.connect('db/aptrust.db')
    conn.row_factory = sqlite3.Row # enables row as dict
    run_report(conn)
    conn.close()
