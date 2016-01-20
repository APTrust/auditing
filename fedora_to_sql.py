#! /usr/bin/env python
# fedora_to_sql.py
"""
Imports JSON data dumped out from our Fedora installation,
using "bundle exec rake fluctus:dump_data". That rake task
creates a small file called institutions.json and a big file
called objects.json. Each file contains one JSON record per
line.
"""
from datetime import datetime
import json
import os
import sqlite3
import sys

def import_json(conn, file_path):
    line_number = 0
    records_saved = 0
    save_function = save_intellectual_object
    # The rake task exports two files: institutions.json
    # and objects.json. We make an assumption here about
    # file names, since this script was written specifically to
    # work with the output of the rake task.
    if "institutions.json" in file_path:
        print "Looks like you're importing institutions"
        save_function = save_institution
    elif not "objects.json" in file_path:
        print "Assuming you're saving Fedora objects/files/events"
    with open(file_path) as f:
        for line in f:
            line_number += 1
            if line_number % 500 == 0:
                print("Processed {0} lines".format(line_number))
            try:
                data = json.loads(line)
            except ValueError as err:
                print("Error decoding JSON on line {0}: {1}".format(line_number, err))
            records_saved += save_function(conn, data)
    print("Processed {0} json records. Saved {1} new/updated records".format(
        line_number, records_saved))

def object_exists(conn, fedora_pid):
    """
    Returns true if an intellectual object with the fedora_pid is
    already in the database.
    """
    return record_exists(conn, 'fedora_objects', 'pid', fedora_pid)

def file_exists(conn, fedora_pid):
    """
    Returns true if a generic file with the fedora_pid is already
    in the database.
    """
    return record_exists(conn, 'fedora_files', 'pid', fedora_pid)

def event_exists(conn, event_uuid):
    """
    Returns true if an event with the event_uuid is already
    in the database.
    """
    return record_exists(conn, 'fedora_events', 'identifier', event_uuid)

def institution_exists(conn, fedora_pid):
    """
    Returns true if an institution with the fedora_pid is already
    in the database.
    """
    return record_exists(conn, 'fedora_institutions', 'pid', fedora_pid)

def record_exists(conn, table, column, value):
    """
    Returns true if a record is already in the database.
    Query should be on a column with a unique index.
    """
    statement = "select exists(select 1 from {0} where {1}=?)".format(
        table, column)
    values = (value)
    cursor = conn.cursor()
    cursor.execute(statement, values)
    result = cursor.fetchone()
    cursor.close()
    return result[0] == 1

def checksum_exists(conn, fedora_file_id, data):
    """
    Returns true if the checksum is already in the database.
    """
    statement = """select exists(select 1 from fedora_checksums
    where fedora_file_id=? and algorithm=? and digest=? and datetime=?)"""
    values = (fedora_file_id,
              data['algorithm'],
              data['digest'],
              data['datetime'])
    cursor = conn.cursor()
    cursor.execute(statement, values)
    result = cursor.fetchone()
    cursor.close()
    return result[0] == 1

def save_institution(conn, data):
    """
    Inserts or updates institution records in the SQL database.
    """
    # TODO: Implement me!
    return 0

def save_intellectual_object(conn, data):
    """
    Inserts or updates intellectual object records in the SQL database.
    This includes, the object, its generic files, and all related events.
    """
    # TODO: Implement me!
    return 0

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

def save_object(conn, data):
    pass

def save_file(conn, data):
    pass

def save_checksum(conn, data):
    pass

def save_event(conn, data):
    pass

def initialize_db(conn):
    """
    Creates the database tables and indexes if they don't already exist.
    """
    query = """SELECT name FROM sqlite_master WHERE type='table'
    AND name='fedora_objects'"""
    c = conn.cursor()
    c.execute(query)
    row = c.fetchone()
    if not row or len(row) < 1:
        print("Creating table fedora_objects")
        statement = """create table fedora_objects(
        id integer primary key autoincrement,
        pid text,
        title text,
        description text,
        access text,
        bag_name text,
        identifier text,
        state text,
        alt_identifier text)"""
        conn.execute(statement)
        conn.commit()

        print("Creating table fedora_files")
        statement = """create table fedora_files(
        id integer primary key autoincrement,
        fedora_object_id int,
        pid text,
        uri text,
        size unsigned big int,
        created datetime,
        modified datetime,
        file_format text,
        identifier text,
        state text,
        alt_identifier text,
        FOREIGN KEY(fedora_object_id) REFERENCES fedora_objects(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table fedora_checksums")
        statement = """create table fedora_checksums(
        id integer primary key autoincrement,
        fedora_file_id int,
        algorithm text,
        digest text,
        created datetime,
        FOREIGN KEY(fedora_file_id) REFERENCES fedora_files(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table fedora_events")
        statement = """create table fedora_events(
        id integer primary key autoincrement,
        fedora_object_id int null,
        fedora_file_id int null,
        identifier text,
        type text,
        date_time datetime,
        detail text,
        outcome text,
        outcome_detail text,
        object text,
        agent text,
        outcome_information text,
        FOREIGN KEY(fedora_object_id) REFERENCES fedora_objects(id)
        FOREIGN KEY(fedora_file_id) REFERENCES fedora_files(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table fedora_institutions")
        statement = """create table fedora_institutions(
        id integer primary key autoincrement,
        pid text,
        name text,
        brief_name text,
        identifier text,
        dpn_uuid text)"""
        conn.execute(statement)
        conn.commit()

        # Indexes
        print("Creating index ix_obj_pid on fedora_objects")
        statement = """create unique index ix_obj_pid on
        fedora_objects(pid)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_obj_identifier on fedora_objects")
        statement = """create unique index ix_obj_identifier on
        fedora_objects(identifier)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_file_pid on fedora_files")
        statement = """create unique index ix_file_pid on
        fedora_files(pid)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_file_identifier on fedora_files")
        statement = """create unique index ix_file_identifier on
        fedora_files(identifier)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_file_object_id on fedora_files")
        statement = """create index ix_file_object_id on
        fedora_files(fedora_object_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_checksum_file_id on fedora_checksums")
        statement = """create index ix_checksum_file_id on
        fedora_checksums(fedora_file_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_events_object_id on fedora_events")
        statement = """create index ix_events_object_id on
        fedora_events(fedora_object_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_events_file_id on fedora_events")
        statement = """create index ix_events_file_id on
        fedora_events(fedora_file_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_events_identifier on fedora_events")
        statement = """create unique index ix_events_identifier on
        fedora_events(identifier)"""
        conn.execute(statement)
        conn.commit()

    c.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Missing arg: path to json data file")
        print("Usage: python logs_to_sql.py <path/to/institutions.json>")
        print("Or...  python logs_to_sql.py <path/to/objects.json>")
        sys.exit()
    if not os.path.exists('db'):
        os.mkdir('db')
    conn = sqlite3.connect('db/aptrust_fedora.db')
    # Turn OFF automatic transactions, because we want to
    # manage these manually.
    conn.isolation_level = None
    initialize_db(conn)
    import_json(conn, sys.argv[1])
    conn.close()
