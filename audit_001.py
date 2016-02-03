# audit_001.py
#
# Audit ingest failures.
#
# Usage: python audit_001.py college.edu.name_of_bag.tar
#
import sqlite3
import sys
import json


def full_object_report(conn, bag_name):
    print("Full report for bag {0}".format(bag_name))
    print_summary_header(conn, bag_name)
    query = """select
    f.ingest_record_id,
    f.unpacked_file_path,
    f.gf_file_path,
    f.gf_needs_save,
    f.gf_identifier,
    f.gf_storage_url,
    f.fedora_file_uri,
    f.gf_uuid,
    f.s3_key,
    f.glacier_key
    from audit_001_objects o
    inner join audit_001_files f on f.ingest_record_id = o.ingest_record_id
    where o.key = ?"""
    values = (bag_name,)
    c = conn.cursor()
    try:
        c.execute(query, values)
        rows = c.fetchall()
        print("{0} rows".format(len(rows)))
        for row in rows:
            for key in row.keys():
                print("{0:20}:  {1}".format(key, row[key]))
            print('-' * 76)
    except (sqlite3.Error, RuntimeError) as err:
        print(err)
    finally:
        c.close()

def print_summary_header(conn, bag_name):
    c = conn.cursor()
    values = (bag_name,)

    query = """select o.error_message from audit_001_objects o where o.key = ?"""
    c.execute(query, values)
    error_message = c.fetchone()[0]
    print("Error: {0}".format(error_message))

    query = """select count(*) from ingest_unpacked_files iuf
    inner join ingest_tar_results itr on itr.id = iuf.ingest_tar_result_id
    inner join ingest_s3_files s3 on s3.ingest_record_id = itr.ingest_record_id
    where s3.key = ? and iuf.file_path like 'data/%'"""
    c.execute(query, values)
    unpacked_file_count = c.fetchone()[0]

    print("Files Unpacked:            {0}".format(unpacked_file_count))

    query = """select count(*) from ingest_generic_files igf
    inner join ingest_tar_results itr on itr.id = igf.ingest_tar_result_id
    inner join ingest_s3_files s3 on s3.ingest_record_id = itr.ingest_record_id
    where s3.key = ?"""
    c.execute(query, values)
    ingest_generic_file_count = c.fetchone()[0]

    print("Generic Files from Ingest: {0}".format(ingest_generic_file_count))

    query = """select count(*) from ingest_fedora_generic_files igf
    inner join ingest_fedora_results ifr on ifr.id = igf.ingest_fedora_result_id
    inner join ingest_s3_files s3 on s3.ingest_record_id = ifr.ingest_record_id
    where s3.key = ?"""
    c.execute(query, values)
    fedora_generic_file_count = c.fetchone()[0]

    print("Generic Files in Fedora:   {0}".format(fedora_generic_file_count))

    bag_name_without_tar = bag_name.rstrip('.tar')
    query = """select count(distinct(m.key_id)) from s3_meta m
    inner join s3_keys k on m.key_id = k.id
    where m.name='bag' and m.value=? and k.bucket='aptrust.preservation.storage'"""
    c.execute(query, (bag_name_without_tar,))
    s3_file_count = c.fetchone()[0]

    print("Generic Files in S3:       {0}".format(s3_file_count))

    query = """select count(distinct(m.key_id)) from s3_meta m
    inner join s3_keys k on m.key_id = k.id
    where m.name='bag' and m.value=? and k.bucket='aptrust.preservation.oregon'"""
    c.execute(query, (bag_name_without_tar,))
    glacier_file_count = c.fetchone()[0]

    print("Generic Files in Glacier:  {0}".format(glacier_file_count))

    c.close()


class ObjectStat:
    def __init__(self, bag_name, error_message):
        self.bag_name = bag_name
        self.error_message = error_message
        self.files = []
    def add_file(self, filestat):
        filestat.check_keys()
        self.files.append(filestat)
    def _summarize(self):
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
        self._summarize()
        return {
            'bag_name': self.bag_name,
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
    def __init__(self, path, size):
        self.path = path
        self.size = size
        self.fedora_url = None
        self.fedora_url_should_be = None
        self.aws_keys = {}
        self.key_to_keep = None
        self.s3_keys_to_delete = []
        self.glacier_keys_to_delete = []
        self.s3_missing_keys = []
        self.glacier_missing_keys = []
    def add_s3_key(self, key):
        locations = self.aws_keys.get(key, [])
        if len(locations) == 0:
            self.aws_keys[key] = locations
        self.aws_keys[key].append('s3')
    def add_glacier_key(self, key):
        locations = self.aws_keys.get(key, [])
        if len(locations) == 0:
            self.aws_keys[key] = locations
        self.aws_keys[key].append('glacier')
    def url_suffix(self):
        return self.fedora_url.split('/')[-1]
    def to_hash(self):
        self.check_keys()
        return {
            'path': self.path,
            'fedora_url': self.fedora_url,
            'fedora_url_should_be': self.fedora_url_should_be,
            'aws_keys': self.aws_keys,
            'key_to_keep': self.key_to_keep,
            's3_keys_to_delete': self.s3_keys_to_delete,
            'glacier_keys_to_delete': self.glacier_keys_to_delete,
            's3_missing_keys': self.s3_missing_keys,
            'glacier_missing_keys': self.glacier_missing_keys,
            'error_message': self.error_message
        }
    def check_keys(self):
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


def report_on_all_files(conn):
    c = conn.cursor()
    query = "select key from audit_001_objects"
    index = 1
    c.execute(query)
    for row in c.fetchall():
        bag_name = row[0]
        sys.stderr.write("{0:4d}  {1}\n".format(index, bag_name))
        print_file_summary(conn, bag_name)
        index += 1
    c.close()

def print_file_summary(conn, bag_name):
    filestat = None
    values = (bag_name,)
    c = conn.cursor()


    query = """select o.error_message from audit_001_objects o where o.key = ?"""
    c.execute(query, values)
    error_message = c.fetchone()[0]

    obj_stat = ObjectStat(bag_name, error_message)

    # Get a list of files that were unpacked from the tar bag.
    query = """select file_path, s3.size from ingest_unpacked_files iuf
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
        query = """select f.fedora_file_uri from audit_001_files f
        inner join ingest_s3_files s3 on s3.ingest_record_id = f.ingest_record_id
        where s3.key = ? and f.gf_file_path = ?"""
        values = (bag_name, filestat.path)
        c.execute(query, values)
        filestat.fedora_url = c.fetchone()[0]

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
    print(json.dumps(obj_stat.to_hash(), sort_keys=True, indent=2))


def unrecorded_file_report(conn, bag_name):
    print("Unrecorded file report for bag {0}".format(bag_name))
    query = """select
    f.ingest_record_id,
    f.unpacked_file_path,
    f.gf_file_path,
    f.gf_needs_save,
    f.gf_identifier,
    f.gf_storage_url,
    f.fedora_file_uri,
    f.gf_uuid,
    f.s3_key,
    f.glacier_key
    from audit_001_objects o
    inner join audit_001_files f on f.ingest_record_id = o.ingest_record_id
    where o.key = ?
    and f.gf_needs_save = 1
    and (f.s3_key is null or f.glacier_key is null or
         f.s3_key != f.gf_uuid or f.glacier_key != f.gf_uuid or
         substr(f.gf_storage_url, 55) != f.gf_uuid or
         substr(f.fedora_file_uri, 55) != f.gf_uuid or
         f.gf_storage_url != f.fedora_file_uri)"""
    values = (bag_name,)
    c = conn.cursor()
    try:
        c.execute(query, values)
        rows = c.fetchall()
        print("{0} rows".format(len(rows)))
        for row in rows:
            for key in row.keys():
                print("{0:20}:  {1}".format(key, row[key]))
            print('-' * 76)
    except (sqlite3.Error, RuntimeError) as err:
        print(err)
    finally:
        c.close()


def duplicate_file_report(conn):
    print("Duplicate file report")
    query = """select
    duplicate_s3,
    duplicate_glacier,
    gf_uuid,
    gf_stored_at,
    substr(gf_storage_url, 55),
    substr(fedora_file_uri, 55),
    s3_key,
    glacier_key,
    gf_identifier
    from audit_001_problem_files
    where duplicate_s3 = 1
    or duplicate_glacier = 1"""
    c = conn.cursor()
    try:
        c.execute(query)
        rows = c.fetchall()
        print("Missing S3\tMissing Glacier\tUUID\tStorage Date\tStorageURL\tFedoraURL\tS3 Key\tGlacier Key\tIdentifier")
        for row in rows:
            print("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}".format(
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        print('-' * 76)
        print("{0} rows".format(len(rows)))
    except (sqlite3.Error, RuntimeError) as err:
        print(err)
    finally:
        c.close()


def missing_file_report(conn):
    print("Missing file report")
    query = """select
    missing_s3,
    missing_glacier,
    gf_uuid,
    gf_stored_at,
    substr(gf_storage_url, 55),
    substr(fedora_file_uri, 55),
    s3_key,
    glacier_key,
    gf_identifier
    from audit_001_problem_files
    where missing_s3 = 1
    or missing_glacier = 1"""
    c = conn.cursor()
    try:
        c.execute(query)
        rows = c.fetchall()
        print("Missing S3\tMissing Glacier\tUUID\tStorage Date\tStorageURL\tFedoraURL\tS3 Key\tGlacier Key\tIdentifier")
        for row in rows:
            print("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}".format(
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        print('-' * 76)
        print("{0} rows".format(len(rows)))
    except (sqlite3.Error, RuntimeError) as err:
        print(err)
    finally:
        c.close()



if __name__ == "__main__":
    conn = sqlite3.connect('db/aptrust.db')
    conn.row_factory = sqlite3.Row
    if len(sys.argv) < 2:
        #duplicate_file_report(conn)
        #missing_file_report(conn)
        report_on_all_files(conn)
    else:
        #full_object_report(conn, sys.argv[1])
        #print('')
        #print('*' * 76)
        #print('')
        #unrecorded_file_report(conn, sys.argv[1])
        print_file_summary(conn, sys.argv[1])
    conn.close()
