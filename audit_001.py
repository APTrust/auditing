# audit_001.py
#
# Audit ingest failures.
#
# Usage: python audit_001.py college.edu.name_of_bag.tar
#
import sqlite3
import sys


def object_report(conn, bag_name):
    print("Checking ingest of bag {0}".format(bag_name))
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
    f.glacier_key,
    o.error_message
    from audit_001_objects o
    inner join audit_001_files f on f.ingest_record_id = o.ingest_record_id
    where o.key = ?"""
    values = (bag_name,)
    c = conn.cursor()
    try:
        c.execute(query, values)
        rows = c.fetchall()
        print("{0} rows", len(rows))
        for row in rows:
            for key in row.keys():
                print("{0:20}:  {1}".format(key, row[key]))
            print('-' * 76)
    except (sqlite3.Error, RuntimeError) as err:
        print(err)
    finally:
        c.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Missing arg: bag_name")
        print("Usage: python audit_001.py college.edu.name_of_bag.tar")
        sys.exit()
    conn = sqlite3.connect('db/aptrust.db')
    conn.row_factory = sqlite3.Row
    object_report(conn, sys.argv[1])
    conn.close()
