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

# We cache institution ids when loading objects
institutions = {}

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
    elif "users.json" in file_path:
        print "Looks like you're importing users"
        save_function = save_user
        cache_institutions(conn)
    elif "processed_items.json" in file_path:
        print "Looks like you're importing processed items"
        save_function = save_work_item
        cache_institutions(conn)
    else:
        print "Assuming you're saving Fedora objects, files and events"
        cache_institutions(conn)
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
            except (sqlite3.Error, RuntimeError) as err:
                print("Insert failed for record {0}/{1}".format(
                    data['id'], data.get('identifier', 'no identifier')))
                print(err)
                conn.execute("rollback")
            if new_id > 0:
                records_saved += 1
    print("Processed {0} json records. Saved {1} new records".format(
        line_number, records_saved))

def object_exists(conn, pid):
    """
    Returns true if an intellectual object with the pid is
    already in the database.
    """
    return record_exists(conn, 'objects', 'pid', pid)

def file_exists(conn, pid):
    """
    Returns true if a generic file with the pid is already
    in the database.
    """
    return record_exists(conn, 'files', 'pid', pid)

def event_exists(conn, event_uuid):
    """
    Returns true if an event with the event_uuid is already
    in the database.
    """
    return record_exists(conn, 'events', 'identifier', event_uuid)

def institution_exists(conn, pid):
    """
    Returns true if an institution with the pid is already
    in the database.
    """
    return record_exists(conn, 'institutions', 'pid', pid)

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

def checksum_exists(conn, file_id, data):
    """
    Returns true if the checksum is already in the database.
    """
    statement = """select exists(select 1 from checksums
    where file_id=? and algorithm=? and digest=? and date_time=?)"""
    values = (file_id,
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
    statement = """insert into institutions(pid, name, brief_name,
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
    statement = """insert into objects(
    pid, institution_id, title, description, access, bag_name,
    identifier, state, alt_identifier) values (?,?,?,?,?,?,?,?,?)
    """
    alt_identifier = None
    if len(data['alt_identifier']) > 0:
        alt_identifier = data['alt_identifier'][0]
    values = (data['id'],
              institution_id(data['identifier']),
              data['title'],
              data['description'],
              data['access'],
              data['bag_name'],
              data['identifier'],
              data['state'],
              alt_identifier)
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
    statement = """insert into files(object_id,
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
    statement = """insert into checksums(file_id,
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
    statement = """insert into events(
    object_id, file_id, identifier, type,
    date_time, detail, outcome, outcome_detail, object,
    agent, outcome_information) values (?,?,?,?,?,?,?,?,?,?,?)
    """
    values = (object_id, file_id, data['identifier'],
              data['type'], data['date_time'], data['detail'],
              data['outcome'], data['outcome_detail'],
              data['object'], data['agent'],
              data['outcome_information'],)
    return do_save(conn, statement, values)

def save_user(conn, data):
    if user_by_email(conn, data['email']):
        return 0
    statement = """insert into users(
    id, email, name, phone_number, institution_id, encrypted_api_secret_key,
    encrypted_password, reset_password_token,
    reset_password_sent_at, remember_created_at,
    sign_in_count, current_sign_in_at, last_sign_in_at,
    current_sign_in_ip, last_sign_in_ip, created_at, updated_at)
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    values = (data['id'],
              data['email'],
              data['name'],
              data['phone_number'],
              institution_by_pid(conn, data['institution_pid']),
              data['encrypted_api_secret_key'],
              data['encrypted_password'],
              None,
              None,
              None,
              0,
              None,
              None,
              None,
              None,
              data['created_at'],
              data['updated_at'],)
    return do_save(conn, statement, values)


def save_work_item(conn, data):
    statement = """insert into work_items(
    id, name, etag, bag_date, bucket, user_id,
    institution_id, file_mod_date, note, action,
    stage, status, outcome, retry, reviewed,
    object_identifier, generic_file_identifier,
    created_at, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    if record_exists(conn, 'work_items', 'id', data['id']):
        return 0
    values = (data['id'],
              data['name'],
              data['etag'].replace('"', ''),
              data['bag_date'],
              data['bucket'],
              user_by_email(conn, data['user']),
              institution_id(data['institution']),
              data['date'],
              data['note'],
              data['action'],
              data['stage'],
              data['status'],
              data['outcome'],
              data['retry'],
              data['reviewed'],
              data['object_identifier'],
              data['generic_file_identifier'],
              data['created_at'],
              data['updated_at'],)
    return do_save(conn, statement, values)


def institution_id(object_identifier):
    "Given an object identifier, returns the institution id"
    if '/' in object_identifier:
        inst_identifier, obj_name = object_identifier.split('/', 1)
    else:
        inst_identifier = object_identifier
        obj_name = None
    institution_id = institutions.get(inst_identifier.lower())
    if institution_id is None:
        raise RuntimeError(
            "No institution object {0}, institution identifier {1}".format(
                object_identifier, inst_identifier))
    return institution_id

def institution_by_pid(conn, pid):
    "Given an institution pid, returns the institution id"
    query = "select id from institutions where pid=?"
    values = (pid,)
    c = conn.cursor()
    c.execute(query, values)
    row = c.fetchone()
    c.close()
    institution_id = None
    if row:
        institution_id = row[0]
    if institution_id is None:
        raise RuntimeError(
            "No institution for pid {0}".format(pid))
    return institution_id

def user_by_email(conn, email):
    query = "select id from users where email=?"
    values = (email,)
    c = conn.cursor()
    c.execute(query, values)
    row = c.fetchone()
    user_id = None
    if row:
        user_id = row[0]
    c.close()
    return user_id

def cache_institutions(conn):
    query = "select id, identifier from institutions"
    cursor = conn.cursor()
    for row in cursor.execute(query):
        # map identifier (e.g. virginia.edu) to primary key
        institutions[row[1].lower()] = row[0]
    cursor.close()
    if len(institutions) == 0:
        raise RuntimeError("You must load institutions before loading objects.")

def initialize_db(conn):
    """
    Creates the database tables and indexes if they don't already exist.
    """
    query = """SELECT name FROM sqlite_master WHERE type='table'
    AND name='objects'"""
    c = conn.cursor()
    c.execute(query)
    row = c.fetchone()
    if not row or len(row) < 1:
        print("Creating table institutions")
        statement = """create table institutions(
        id integer primary key autoincrement,
        pid text,
        name text,
        brief_name text,
        identifier text,
        dpn_uuid text)"""
        conn.execute(statement)
        conn.commit()

        print("Creating table users")
        statement = """create table users(
        id integer primary key autoincrement,
        email varchar(255) not null,
        name varchar(255),
        phone_number varchar(80),
        institution_id integer,
        encrypted_api_secret_key varchar(255),
        encrypted_password varchar(255),
        reset_password_token varchar(255),
        reset_password_sent_at datetime,
        remember_created_at datetime,
        sign_in_count integer,
        current_sign_in_at datetime,
        last_sign_in_at datetime,
        current_sign_in_ip varchar(40),
        last_sign_in_ip varchar(40),
        created_at datetime,
        updated_at datetime,
        FOREIGN KEY(institution_id)
        REFERENCES institutions(id));
        """
        conn.execute(statement)
        conn.commit()

        print("Creating table work_items")
        statement = """create table work_items(
        id integer primary key autoincrement,
        name varchar(255),
        etag varchar(80),
        bag_date datetime,
        bucket varchar(255),
        user_id integer,
        institution_id integer,
        file_mod_date datetime,
        note text,
        action varchar(40),
        stage varchar(40),
        status varchar(40),
        outcome varchar(40),
        retry boolean,
        reviewed boolean,
        object_identifier varchar(255),
        generic_file_identifier varchar(255),
        created_at datetime,
        updated_at datetime,
        FOREIGN KEY(institution_id)
        REFERENCES institutions(id));"""
        conn.execute(statement)
        conn.commit()

        print("Creating table objects")
        statement = """create table objects(
        id integer primary key autoincrement,
        institution_id int not null,
        pid text,
        title text,
        description text,
        access text,
        bag_name text,
        identifier text,
        state text,
        alt_identifier text,
        FOREIGN KEY(institution_id) REFERENCES institutions(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table files")
        statement = """create table files(
        id integer primary key autoincrement,
        object_id int,
        pid text,
        uri text,
        size unsigned big int,
        created datetime,
        modified datetime,
        file_format text,
        identifier text,
        state text,
        FOREIGN KEY(object_id) REFERENCES objects(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table checksums")
        statement = """create table checksums(
        id integer primary key autoincrement,
        file_id int,
        algorithm text,
        digest text,
        date_time datetime,
        FOREIGN KEY(file_id) REFERENCES files(id))"""
        conn.execute(statement)
        conn.commit()

        print("Creating table events")
        statement = """create table events(
        id integer primary key autoincrement,
        object_id int null,
        file_id int null,
        identifier text,
        type text,
        date_time datetime,
        detail text,
        outcome text,
        outcome_detail text,
        object text,
        agent text,
        outcome_information text,
        FOREIGN KEY(object_id) REFERENCES objects(id)
        FOREIGN KEY(file_id) REFERENCES files(id))"""
        conn.execute(statement)
        conn.commit()

        # Indexes
        print("Creating index ix_obj_pid on objects")
        statement = """create unique index ix_obj_pid on
        objects(pid)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_obj_identifier on objects")
        statement = """create unique index ix_obj_identifier on
        objects(identifier)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_file_pid on files")
        statement = """create unique index ix_file_pid on
        files(pid)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_file_identifier on files")
        statement = """create unique index ix_file_identifier on
        files(identifier)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_file_object_id on files")
        statement = """create index ix_file_object_id on
        files(object_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_checksum_file_id on checksums")
        statement = """create index ix_checksum_file_id on
        checksums(file_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_events_object_id on events")
        statement = """create index ix_events_object_id on
        events(object_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_events_file_id on events")
        statement = """create index ix_events_file_id on
        events(file_id)"""
        conn.execute(statement)
        conn.commit()

        print("Creating index ix_events_identifier on events")
        statement = """create unique index ix_events_identifier on
        events(identifier)"""
        conn.execute(statement)
        conn.commit()

    c.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Missing arg: path to json data file")
        print("Usage: python fedora_to_sql.py <path/to/institutions.json>")
        print("Or...  python fedora_to_sql.py <path/to/users.json>")
        print("Or...  python fedora_to_sql.py <path/to/processed_items.json>")
        print("Or...  python fedora_to_sql.py <path/to/objects.json>")
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
