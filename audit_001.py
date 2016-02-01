# audit_001.py
#
# Audit ingest failures.
#
# Usage: python audit_001.py college.edu.name_of_bag.tar
#
import sqlite3
import sys


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


class FileStat:
    def __init__(self, path):
        self.path = path
        self.fedoda_url = None
        self.s3_keys = []
        self.glacier_keys = []
        self.has_error = False
        self.report = ''
    def add_s3_key(self, key):
        self.s3_keys.append(key)
    def add_glacier_key(self, key):
        self.glacier_keys.append(key)
    def url_suffix(self):
        return self.fedora_url.split('/')[-1]
    def print_report(self):
        self.report += "Path\n  {0}\n".format(self.path)
        self.report += "URL\n  {0}\n".format(self.fedora_url)
        self._check_keys('S3', self.s3_keys)
        self._check_keys('Glacier', self.glacier_keys)
        if self.has_error:
            print(self.report)
        else:
            print('[OK] {0} -> {1}'.format(self.path, self.fedora_url))
        print('-' * 72)
    def _check_keys(self, name, collection):
        fedora_key = self.url_suffix()
        fedora_key_found = False
        self.report += "{0} Keys\n".format(name)
        for key in collection:
            arrow = "\n"
            if key not in self.fedora_url:
                arrow = "  <--- Extra\n"
                self.has_error = True
            if key == fedora_key:
                fedora_key_found = True
            self.report += "  {0} {1}".format(key, arrow)
        if fedora_key_found == False:
            self.report += "{0} <--- Fedora key missing\n".format(' ' * 40)
            self.has_error = True

def print_file_summary(conn, bag_name):
    files = []
    values = (bag_name,)
    c = conn.cursor()

    print("Full report for bag {0}".format(bag_name))

    query = """select o.error_message from audit_001_objects o where o.key = ?"""
    c.execute(query, values)
    error_message = c.fetchone()[0]
    print("Error: {0}".format(error_message))

    # Get a list of files that were unpacked from the tar bag.
    query = """select file_path from ingest_unpacked_files iuf
    inner join ingest_tar_results itr on itr.id = iuf.ingest_tar_result_id
    inner join ingest_s3_files s3 on s3.ingest_record_id = itr.ingest_record_id
    where s3.key = ? and iuf.file_path like 'data/%'"""
    c.execute(query, values)
    rows = c.fetchall()
    for row in rows:
        files.append(FileStat(row[0]))


    bag_name_without_tar = bag_name.rstrip('.tar')

    # For each file...
    for filestat in files:
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
        where m.name='bag' and k.bucket='aptrust.preservation.storage'
        and m.value = ? and m2.name='bagpath' and m2.value = ?"""
        values = (bag_name_without_tar, filestat.path)
        c.execute(query, values)
        rows = c.fetchall()
        for row in rows:
            filestat.add_s3_key(row[0])

        # ... get the keys stored in Glacier
        query = """select k.name, m2.value from s3_keys k
        inner join s3_meta m on m.key_id = k.id
        inner join s3_meta m2 on m2.key_id = k.id
        where m.name='bag' and k.bucket='aptrust.preservation.oregon'
        and m.value = ? and m2.name='bagpath' and m2.value = ?"""
        values = (bag_name_without_tar, filestat.path)
        c.execute(query, values)
        rows = c.fetchall()
        for row in rows:
            filestat.add_glacier_key(row[0])

        filestat.print_report()

    c.close()


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
        missing_file_report(conn)
    else:
        #full_object_report(conn, sys.argv[1])
        #print('')
        #print('*' * 76)
        #print('')
        #unrecorded_file_report(conn, sys.argv[1])
        print_file_summary(conn, sys.argv[1])
    conn.close()
