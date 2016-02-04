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

def report_on_all_files(conn):
    """
    Run a full report on all files belonging to all of the
    "failed" ingest bags.
    """
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
    """
    Prints a JSON summary to stdout (which you can redirect to a file)
    for the specified bag. The summary includes information about every
    file in that bag, including what ingest services knows about the file,
    what Fedora knows about, and what S3 knows about.
    """
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


if __name__ == "__main__":
    conn = sqlite3.connect('db/aptrust.db')
    conn.row_factory = sqlite3.Row
    if len(sys.argv) < 2:
        report_on_all_files(conn)
    else:
        print_file_summary(conn, sys.argv[1])
    conn.close()
