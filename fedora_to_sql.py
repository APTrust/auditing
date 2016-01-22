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
        print "Assuming you're saving Fedora objects, files and events"
    with open(file_path) as f:
        for line in f:
            new_id = 0
            line_number += 1
            if line_number % 500 == 0:
                print("Processed {0} lines".format(line_number))
            try:
                data = json.loads(line)
            except ValueError as err:
                print("Error decoding JSON on line {0}: {1}".format(line_number, err))
            try:
                conn.execute("begin")
                new_id = save_function(conn, data)
                conn.execute("commit")
            except sqlite3.Error as err:
                print("Insert failed for record {0}/{1}".format(
                    data['id'], data['identifier']))
                print(err)
                conn.execute("rollback")
            if new_id > 0:
                records_saved += 1
    print("Processed {0} json records. Saved {1} new records".format(
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
    Returns true if the record if it exists.
    Query should be on a column with a unique index.
    """
    statement = "select exists(select 1 from {0} where {1}=?)".format(
        table, column)
    values = (value,)
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
    where fedora_file_id=? and algorithm=? and digest=? and date_time=?)"""
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
    if institution_exists(conn, data['pid']):
        return 0
    statement = """insert into fedora_institutions(pid, name, brief_name,
    identifier, dpn_uuid) values (?,?,?,?,?)
    """
    values = (data['pid'], data['name'], data['brief_name'],
              data['identifier'], data['dpn_uuid'])
    return do_save(conn, statement, values)

def save_intellectual_object(conn, data):
    """
    Inserts or updates intellectual object records in the SQL database.
    This includes, the object, its generic files, and all related events.
    """
    if object_exists(conn, data['id']):
        print("Object {0} already exists in DB".format(data['id']))
        return 0
    statement = """insert into fedora_objects(
    pid, title, description, access, bag_name,
    identifier, state, alt_identifier) values (?,?,?,?,?,?,?,?)
    """
    alt_identifier = None
    if len(data['alt_identifier']) > 0:
        alt_identifier = data['alt_identifier'][0]
    values = (data['id'],data['title'],data['description'],
              data['access'],data['bag_name'],data['identifier'],
              data['state'], alt_identifier)
    object_id = do_save(conn, statement, values)
    if data['premisEvents'] is not None:
        for event in data['premisEvents']:
            event_id = save_event(conn, event, object_id, None)
    if data['generic_files'] is not None:
        for generic_file in data['generic_files']:
            generic_file_id = save_file(conn, generic_file, object_id)
    return object_id

def do_save(conn, statement, values):
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

def save_file(conn, data, object_id):
    """
    Saves a Generic File and it's checksums and PREMIS events.
    """
    if file_exists(conn, data['id']):
        return 0
    statement = """insert into fedora_files(fedora_object_id,
    pid, uri, size, created, modified, file_format, identifier,
    state) values (?,?,?,?,?,?,?,?,?)
    """
    values = (object_id, data['id'], data['uri'],
              data['size'], data['created'], data['modified'],
              data['file_format'], data['identifier'],
              data['state'],)
    file_id = do_save(conn, statement, values)
    if data['checksum'] is not None:
        for checksum in data['checksum']:
            checksum_id = save_checksum(conn, checksum, file_id)
    if data['premisEvents'] is not None:
        for event in data['premisEvents']:
            event_id = save_event(conn, event, object_id, file_id)
    return file_id

def save_checksum(conn, data, generic_file_id):
    """
    Saves a checksum, which belongs to a single Generic File.
    """
    if checksum_exists(conn, generic_file_id, data):
        return 0
    statement = """insert into fedora_checksums(fedora_file_id,
    algorithm, digest, date_time) values (?,?,?,?)
    """
    values = (generic_file_id, data['algorithm'],
              data['digest'], data['datetime'],)
    return do_save(conn, statement, values)

def save_event(conn, data, object_id, file_id):
    """
    Saves a PREMIS event. All events should have an object_id.
    Events related to a specific file (most events) will also
    have a file_id.
    """
    if event_exists(conn, data['identifier']):
        return 0
    statement = """insert into fedora_events(
    fedora_object_id, fedora_file_id, identifier, type,
    date_time, detail, outcome, outcome_detail, object,
    agent, outcome_information) values (?,?,?,?,?,?,?,?,?,?,?)
    """
    values = (object_id, file_id, data['identifier'],
              data['type'], data['date_time'], data['detail'],
              data['outcome'], data['outcome_detail'],
              data['object'], data['agent'],
              data['outcome_information'],)
    return do_save(conn, statement, values)

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
        FOREIGN KEY(fedora_object_id) REFERENCES fedora_objects(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table fedora_checksums")
        statement = """create table fedora_checksums(
        id integer primary key autoincrement,
        fedora_file_id int,
        algorithm text,
        digest text,
        date_time datetime,
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
    #conn.row_factory = sqlite3.Row
    initialize_db(conn)
    import_json(conn, sys.argv[1])
    conn.close()
