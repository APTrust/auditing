# audit_001.py
#
# This script uses the aptrust.db database to generate
# reports on audit ingest failures. It assumes you have
# already built the audit database. See the README for
# instructions on that.
#
# To report on a single bag:
#
# python audit_001.py college.edu.name_of_bag.tar
#
# To report on all bags and dump the results to a JSON file:
#
# python audit_001.py > audit_output.json
#
import argparse
import sqlite3
import sys
import json

class ObjectStat:
    """
    ObjectStat collects information about a bag (intellectual object)
    that appears to have failed ingest. Note *appears*. None of these
    bags actually failed ingest. All items were ingested, though some
    files were stored twice in S3 and/or Glacier, while some were
    stored in S3 but not in Glacier.
    """
    def __init__(self, bag_name, error_message):
        self.bag_name = bag_name
        self.error_message = error_message
        self.identifier = None
        self.files = []

    def add_file(self, filestat):
        filestat.check_keys()
        self.files.append(filestat)

    def _summarize(self):
        """
        Internal method to calculate a summary of what happened with
        this object. Which files were stored twice? Which are missing
        the Glacier backup? Which should have the storage URL changed?
        """
        self.total_size = 0
        self.total_files = 0
        self.files_not_ingested = 0
        self.ingested_size = 0
        self.bytes_not_ingested = 0
        self.files_not_ingested = 0
        self.totally_ok = 0
        self.ok_but_needs_deletions = 0
        self.no_url = 0
        self.url_needs_change = 0
        self.s3_deletion_files = 0
        self.s3_deletion_keys = 0
        self.glacier_deletion_files = 0
        self.glacier_deletion_keys = 0
        self.keys_missing_from_s3 = 0
        self.keys_missing_from_glacier = 0
        for f in self.files:
            f.check_keys()
            self.total_size += f.size
            self.total_files += 1
            self.ingested_size += f.size
            self.keys_missing_from_s3 += len(f.s3_missing_keys)
            self.keys_missing_from_glacier += len(f.glacier_missing_keys)
            if f.fedora_url is None:
                self.no_url += 1
                self.ingested_size -= f.size
                self.bytes_not_ingested += f.size
                self.files_not_ingested += 1
            elif f.fedora_url == f.fedora_url_should_be:
                if not f.s3_keys_to_delete and not f.glacier_keys_to_delete:
                    self.totally_ok += 1
                else:
                    self.ok_but_needs_deletions += 1
            else:
                self.url_needs_change += 1
            if f.s3_keys_to_delete:
                self.s3_deletion_files += 1
                self.s3_deletion_keys += len(f.s3_keys_to_delete)
            if f.glacier_keys_to_delete:
                self.glacier_deletion_files += 1
                self.glacier_deletion_keys += len(f.glacier_keys_to_delete)

    def to_hash(self):
        """
        Returns essential data from this object (and its files list) in the
        form of a hash that can be easily serialized to JSON and dumped into
        a file. Some key names in the summary section don't match the property
        names. That's by design, to make it easier to grep for specific
        properties in the JSON dump. The altered JSON names don't conflict
        with names of FileStat properties.
        """
        self._summarize()
        return {
            'bag_name': self.bag_name,
            'identifier': self.identifier,
            'error_message': self.error_message,
            'files': list(map(lambda f:f.to_hash(), self.files)),
            'summary': {
                'total_size': self.total_size,
                'total_files': self.total_files,
                'files_not_ingested': self.files_not_ingested,
                'ingested_size': self.ingested_size,
                'bytes_not_ingested': self.bytes_not_ingested,
                'totally_ok': self.totally_ok,
                'ok_but_needs_deletions': self.ok_but_needs_deletions,
                'no_url': self.no_url,
                'url_needs_change': self.url_needs_change,
                's3_deletion_files': self.s3_deletion_files,
                's3_deletion_keys': self.s3_deletion_keys,
                'glacier_deletion_files': self.glacier_deletion_files,
                'glacier_deletion_keys': self.glacier_deletion_keys,
                'keys_missing_from_s3': self.keys_missing_from_s3,
                'keys_missing_from_glacier': self.keys_missing_from_glacier
            }
        }


class FileStat:
    """
    Contains information about a file that was unpacked from a tarred bag,
    including 1) what the ingest services know about the file, 2) what Fedora
    knows about the file and 3) what S3 and Glacier know about the file.
    """
    def __init__(self, path, size):
        self.path = path
        self.size = size
        self.identifier = None
        self.fedora_url = None
        self.fedora_url_should_be = None
        self.aws_keys = {}
        self.key_to_keep = None
        self.s3_keys_to_delete = []
        self.glacier_keys_to_delete = []
        self.s3_missing_keys = []
        self.glacier_missing_keys = []

    def add_s3_key(self, key):
        """
        Add an S3 key for this file. This is a UUID that serves
        as the S3 key (file name) of the file in our S3 preservation
        storage bucket.
        """
        locations = self.aws_keys.get(key, [])
        if len(locations) == 0:
            self.aws_keys[key] = locations
        self.aws_keys[key].append('s3')

    def add_glacier_key(self, key):
        """
        Add a Glacier key for this file. This is a UUID that serves
        as the key (file name) of this object in our Oregon Glacier
        storage.
        """
        locations = self.aws_keys.get(key, [])
        if len(locations) == 0:
            self.aws_keys[key] = locations
        self.aws_keys[key].append('glacier')

    def url_suffix(self):
        """
        Returns the last component of the Fedora URL for this file.
        That is a UUID, which is the key (file name) of the object
        in our long-term S3 storage area. (It *should* also be the
        key/file name of the object in our Glacier storage, but in
        some cases, the file never made it to Glacier.)
        """
        return self.fedora_url.split('/')[-1]

    def to_hash(self):
        """
        Returns a hash of this object's essential data, so it can be
        easily serialized to JSON.
        """
        self.check_keys()
        return {
            'path': self.path,
            'fedora_url': self.fedora_url,
            'fedora_url_should_be': self.fedora_url_should_be,
            'identifier': self.identifier,
            'aws_keys': self.aws_keys,
            'key_to_keep': self.key_to_keep,
            's3_keys_to_delete': self.s3_keys_to_delete,
            'glacier_keys_to_delete': self.glacier_keys_to_delete,
            's3_missing_keys': self.s3_missing_keys,
            'glacier_missing_keys': self.glacier_missing_keys,
            'error_message': self.error_message
        }

    def check_keys(self):
        """
        This method compares data from ingest services, Fedora and AWS to
        determine 1) whether the Fedora URL is correct, 2) what the Fedora
        URL should be, 3) which files never made it to Glacier, and
        4) which files should be deleted from S3/Glacier because they are
        duplicates from a subsequent ingest attempt.
        """
        if self.fedora_url is None:
            return
        self.key_to_keep = None
        self.error_message = None
        fedora_key = self.url_suffix()
        # If the key in the Fedora URL is stored in both S3 and Glacier,
        # keep that key and delete all others. For this file, storage
        # was successful.
        for key, locations in self.aws_keys.iteritems():
            if key in self.fedora_url and 's3' in locations and 'glacier' in locations:
                self.key_to_keep = key
                self.fedora_url_should_be = self.fedora_url
        # We have many cases where the storage URL includes a key that
        # was stored in S3 but not Glacier... and then the item was
        # stored again under a new key in both S3 and Glacier. For those
        # items, switch the URL to the new key. We'll get rid of the old
        # URL and the item in S3 it points to.
        if self.key_to_keep is None:
            for key, locations in self.aws_keys.iteritems():
                if key not in self.fedora_url and 's3' in locations and 'glacier' in locations:
                    self.key_to_keep = key
                    self.fedora_url_should_be = self.fedora_url.replace(fedora_key, key)

        # We hit this case when we have the key stored in S3 but not in Glacier.
        if self.fedora_url_should_be is None:
            self.fedora_url_should_be = self.fedora_url

        # Now, if we have an authoritative key, we want to delete the other
        # items from S3/Glacier.
        for key, locations in self.aws_keys.iteritems():
            if key not in self.fedora_url_should_be:
                if 's3' in locations:
                    self.s3_keys_to_delete.append(key)
                if 'glacier' in locations:
                    self.glacier_keys_to_delete.append(key)
            else: # key matches URL
                if 's3' not in locations:
                    self.s3_missing_keys.append(key)
                if 'glacier' not in locations:
                    self.glacier_missing_keys.append(key)

def report_on_all_files(read_conn, write_conn, output_type):
    """
    Run a full report on all files belonging to all of the
    "failed" ingest bags.
    """
    c = read_conn.cursor()
    query = "select key from audit_001_objects"
    index = 1
    c.execute(query)
    for row in c.fetchall():
        bag_name = row[0]
        sys.stderr.write("{0:4d}  {1}\n".format(index, bag_name))
        build_summary(read_conn, write_conn, bag_name, output_type)
        index += 1
    c.close()

def build_summary(read_conn, write_conn, bag_name, output_type):
    """
    Builds a summary of the state of an object and its files, including
    any issues. If output_type is 'json', this prints a JSON summary to
    stdout (which you can redirect to a file) for the specified bag.
    The summary includes information about every file in that bag, including
    what ingest services knows about the file, what Fedora knows about,
    and what S3 knows about. If output_type is anything other than JSON,
    this saves the data to the audit001_summary.db database.
    """
    filestat = None
    values = (bag_name,)
    c = read_conn.cursor()

    query = """select o.error_message, object_identifier from
    audit_001_objects o where o.key = ?"""
    c.execute(query, values)
    row = c.fetchone()
    error_message = row[0]

    obj_stat = ObjectStat(bag_name, error_message)
    obj_stat.identifier = row[1]

    # Get a list of files that were unpacked from the tar bag.
    query = """select iuf.file_path, s3.size from ingest_unpacked_files iuf
    inner join ingest_tar_results itr on itr.id = iuf.ingest_tar_result_id
    inner join ingest_s3_files s3 on s3.ingest_record_id = itr.ingest_record_id
    where s3.key = ? and iuf.file_path like 'data/%'"""
    c.execute(query, values)
    rows = c.fetchall()
    for row in rows:
        filestat = FileStat(row[0], row[1])
        obj_stat.add_file(filestat)


    bag_name_without_tar = bag_name.rstrip('.tar')

    # For each file...
    for filestat in obj_stat.files:
        # ... get the URL stored in Fedora
        query = query = """select f.fedora_file_uri, f.gf_identifier from audit_001_files f
        inner join ingest_s3_files s3 on s3.ingest_record_id = f.ingest_record_id
        where s3.key = ? and f.gf_file_path = ?"""
        values = (bag_name, filestat.path)
        c.execute(query, values)
        row = c.fetchone()
        filestat.fedora_url = row[0]
        filestat.identifier = row[1]

        # ... get the keys stored in S3
        query = """select k.name, m2.value from s3_keys k
        inner join s3_meta m on m.key_id = k.id
        inner join s3_meta m2 on m2.key_id = k.id
        where k.bucket='aptrust.preservation.storage'
        and m.name='bag' and m.value = ?
        and m2.name='bagpath' and m2.value = ?"""
        values = (bag_name_without_tar, filestat.path)
        c.execute(query, values)
        rows = c.fetchall()
        for row in rows:
            filestat.add_s3_key(row[0])

        # ... get the keys stored in Glacier
        query = """select k.name, m2.value from s3_keys k
        inner join s3_meta m on m.key_id = k.id
        inner join s3_meta m2 on m2.key_id = k.id
        where k.bucket='aptrust.preservation.oregon'
        and m.name='bag' and m.value = ?
        and m2.name='bagpath' and m2.value = ?"""
        values = (bag_name_without_tar, filestat.path)
        c.execute(query, values)
        rows = c.fetchall()
        for row in rows:
            filestat.add_glacier_key(row[0])

    c.close()

    if output_type == 'json':
        print(json.dumps(obj_stat.to_hash(), sort_keys=True, indent=2))
    else:
        save_to_db(write_conn, obj_stat)


def save_to_db(write_conn, obj_stat):
    bag_id = save_bag(write_conn, obj_stat.bag_name, obj_stat.identifier)
    for filestat in obj_stat.files:
        filestat.check_keys()
        for uuid in filestat.s3_keys_to_delete:
            save_key(write_conn,
                     bag_id,
                     'delete',
                     's3',
                     filestat.path,
                     filestat.identifier,
                     uuid)
        for uuid in filestat.glacier_keys_to_delete:
            save_key(write_conn,
                     bag_id,
                     'delete',
                     'glacier',
                     filestat.path,
                     filestat.identifier,
                     uuid)
        for uuid in filestat.s3_missing_keys:
            save_key(write_conn,
                     bag_id,
                     'add',
                     's3',
                     filestat.path,
                     filestat.identifier,
                     uuid)
        for uuid in filestat.glacier_missing_keys:
            save_key(write_conn,
                     bag_id,
                     'add',
                     'glacier',
                     filestat.path,
                     filestat.identifier,
                     uuid)
        if filestat.fedora_url != filestat.fedora_url_should_be:
            save_url(write_conn,
                     bag_id,
                     filestat.path,
                     filestat.identifier,
                     filestat.fedora_url,
                     filestat.fedora_url_should_be)

def save_bag(write_conn, bag_name, bag_identifier):
    bag_id = None
    query = "select id from bags where name=?"
    values = (bag_name,)
    cursor = write_conn.cursor()
    cursor.execute(query, values)
    result = cursor.fetchone()
    if result and result[0]:
        bag_id = result[0]
    if bag_id == None:
        statement = "insert into bags(name, identifier) values (?, ?)"
        values = (bag_name, bag_identifier,)
        cursor.execute(statement, values)
        write_conn.commit()
        bag_id = cursor.lastrowid
    cursor.close()
    return bag_id

def save_key(write_conn, bag_id, action, storage, file_path, identifier, key):
    key_id = None
    query = """select id from aws_files where bag_id=? and action=?
    and storage=? and file_path=? and key=?"""
    values = (bag_id, action, storage, file_path, key)
    cursor = write_conn.cursor()
    cursor.execute(query, values)
    result = cursor.fetchone()
    if result and result[0]:
        key_id = result[0]
    if key_id == None:
        statement = """insert into aws_files(bag_id, action,
        action_completed_at, storage, file_path, identifier, key)
        values (?,?,?,?,?,?,?)"""
        values = (bag_id, action, None, storage, file_path, identifier, key)
        cursor.execute(statement, values)
        write_conn.commit()
        bag_id = cursor.lastrowid
    cursor.close()
    return key_id


def save_url(write_conn, bag_id, file_path, identifier, old_url, new_url):
    url_id = None
    query = "select id from urls where bag_id=? and file_path=?"
    values = (bag_id, file_path,)
    cursor = write_conn.cursor()
    cursor.execute(query, values)
    result = cursor.fetchone()
    if result and result[0]:
        url_id = result[0]
    if url_id == None:
        statement = """insert into urls(bag_id, file_path, identifier,
        old_url, new_url) values (?,?,?,?,?)"""
        values = (bag_id, file_path, identifier, old_url, new_url)
        cursor.execute(statement, values)
        write_conn.commit()
        url_id = cursor.lastrowid
    cursor.close()
    return url_id


def create_db(write_conn):
    query = """SELECT name FROM sqlite_master WHERE type='table'
    AND name='bags'"""
    c = write_conn.cursor()
    c.execute(query)
    row = c.fetchone()
    if row and row[0]:
        print("Dropping old tables")
        statement = "drop table bags"
        write_conn.execute(statement)
        write_conn.commit()
        statement = "drop table urls"
        write_conn.execute(statement)
        write_conn.commit()
        statement = "drop table aws_files"
        write_conn.execute(statement)
        write_conn.commit()


    print("Creating table bags")
    statement = """create table bags(
    id integer primary key autoincrement,
    name varchar(255) not null,
    identifier varchar(255) not null)"""
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating unique index ix_bag_name_unique on bags")
    statement = "create unique index ix_bag_name_unique on bags(name)"
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating unique index ix_bag_identifier_unique on bags")
    statement = "create unique index ix_bag_identifier_unique on bags(identifier)"
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating table urls")
    statement = """create table urls(
    id integer primary key autoincrement,
    bag_id integer not null,
    file_path varchar(255),
    identifier varchar(255) not null,
    old_url varchar(255),
    new_url varchar(255),
    FOREIGN KEY(bag_id)
    REFERENCES bags(id));"""
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating unique index ix_urls_bag_id_file_path on urls")
    statement = """create unique index ix_urls_bag_id_file_path
    on urls(bag_id, file_path)"""
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating index ix_urls_identifier on urls")
    statement = "create index ix_urls_identifier on urls(identifier)"
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating table aws_files")
    statement = """create table aws_files(
    id integer primary key autoincrement,
    bag_id integer not null,
    action varchar(40) not null,
    action_completed_at datetime null,
    storage varchar(20) not null,
    file_path varchar(255) not null,
    identifier varchar(255) not null,
    key varchar(80) not null,
    FOREIGN KEY(bag_id)
    REFERENCES bags(id));"""
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating index ix_aws_files_file_path on aws_files")
    statement = "create index ix_aws_files_file_path on aws_files(file_path)"
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating index ix_aws_files_key on aws_files")
    statement = "create index ix_aws_files_key on aws_files(key)"
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating index ix_aws_files_bag_id on urls")
    statement = "create index ix_aws_files_bag_id on aws_files(bag_id)"
    write_conn.execute(statement)
    write_conn.commit()

    print("Creating index ix_aws_files_identifier on aws_files")
    statement = "create index ix_aws_files_identifier on aws_files(identifier)"
    write_conn.execute(statement)
    write_conn.commit()

    c.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run audit on ingest errors')
    parser.add_argument('--output', default='sql',
                        help="Output 'json' or 'sql'")
    parser.add_argument("bag_name", nargs='?')
    args = parser.parse_args()

    if args.output != 'sql' and args.output != 'json':
        print("Option --output must be either 'json' or 'sql'")
        sys.exit(0)
    if args.output == 'sql' and args.bag_name:
        print("I don't do sql for just one bag. Try omitting the bag name.")
        sys.exit(0)

    read_conn = sqlite3.connect('db/aptrust.db')
    read_conn.row_factory = sqlite3.Row
    write_conn = sqlite3.connect('db/audit001_summary.db')
    if args.bag_name:
        build_summary(read_conn, write_conn, args.bag_name, args.output)
    else:
        if args.output == 'sql':
            create_db(write_conn)
        report_on_all_files(read_conn, write_conn, args.output)
    read_conn.close()
    write_conn.close()
