# APTrust Auditing

This repository contains scripts for auditing APTrust logs, S3 buckets and
Fedora metadata. The scripts load data from those three sources into a SQL
database so we can compare what we've attempted to ingest against what's
stored in S3 against what Fedora says we've stored.

The audit_001.py script does the analysis by running a set of queries and
cross checks on a SQLite database in db/aptrust.db.

Note that audit_001.py specifically addresses the needs of our first
audit, which is to track down the actual state of a number of bags and files
after Fluctus sent a number of invalid HTTP responses to our ingest
services. The general issue here is that, although most ingests were
successful, Fluctus was not able to tell our ingest services that the
data was correctly recorded. So ingest services assumed the ingests failed
and marked a number of items for review.

Future audits will likely have to deal with different scenarios, and may
not have anything to do with problems. We may simply be asked by our
depositors to prove that a number of bags they sent us a) were correctly
recorded in Fedora and b) are correctly stored in S3 and Glacier.

The aptrust.db we can build with the scripts in this repository allow us
to do that at any time.

## A Note on APTrust's JSON Logs

APTrust's ingest services generate two sets of logs. Files ending in
.log are human-readable, and are suitable for searching and tailing.
Files ending in .json are machine readable and have three purposes:

1. They contain a full manifest of everything the ingest services
know about a bag, including where it came from, what files are in
it, how far the system got in processing the bag and its files,
and what, if anything, went wrong during processing.

2. They can be fed back into the service queues so that ingest
services can resume processing the bag where they left off. (This
is useful in cases where an external service, such as Fluctus/Fedora
fails. We fix the problem in the external system, then feed all the
JSON files back into the queue for reprocessing. No work is lost
or repeated.)

3. They can be used for auditing both Fedora and our AWS storage.

The apt_record.json logs contain difinitive information about what
Fedora *should* know about each bag. They include a full manifest of
every file that was unpacked from the origin tar bag, every checksum,
every tag that was parsed, every file UUID, and where items were
stored in S3 before being recorded.

The fact that a bag record appears in the apt_record.json log means:

    * the bag was successfully downloaded and unpacked from the
      receiving bucket
    * the bag was valid, with all files present and all checksums
      verified
    * identifiers (UUIDs) were assigned to every file in the bag's
      data directory
    * every file in the bag's data directory was successfully stored
      in S3 long-term storage

Each entry in the apt_record.json log contains all of the information
necessary to create all of the PREMIS events associated with ingest.
If an entry contains a non-empty error message, then some part of the
process of recording ingest data in Fedora/Fluctus failed. The
recording failure implies a few possibilities:

    * the intellectual object may not be recorded in Fedora
    * some or all of the object's generic files may not be recorded
      in Fedora
    * some or all of the object's generic files may not have been
      copied to Glacier, because the Fedora recording error stops
      further processing, and copying to Glacier is the last step
      of the ingest process (after Fedora recording)


## Building aptrust.db

The auditing database includes information collected from a number of
sources. It's fairly large (starting at 2GB), so you probably want to
pull all the data down to your local machine, where you have plenty
of CPU and memory to run queries.

1. Dump data from Fedora into JSON format by running the following rake
tasks on the live server:

    * `bundle exec rake fluctus:dump_users`
    * `bundle exec rake fluctus:dump_processed_items`
    * `bundle exec rake fluctus:dump_data`

2. Copy those JSON files from the live server to the data directory of
this repository and run the fedora_to_sql.py script. This will create a
SQLite database in the db directory called aptrust_fedora.sql.

3. Load all of our S3 and Glacier entries into a SQLite db by running
the s3_buckets_to_sql.py script. This script can take over 24 hours to
run, so you might want to run it on the APTrust util server (apt-util).
Note that it expects to find our AWS credentials in the environment.
This script will produce a SQLite database called aptrust_s3.db.

4. Copy aptrust_s3.db into the db directory of this repo. (Don't add it
to GitHub! The whole db directory should be in .gitignore, because the
databases are big, they might contain sensitive information, and they
are meant to be regenerated from scratch every time we want to perform
an audit.)

5. Load the JSON logs from the ingest server onto a single machine.
Specifically, you want the apt_record.json logs from /mnt/apt/logs
on the apt-live-ingest server. Some of these logs get backed up with
timestamps and have names like apt_record.json-20150928. These logs
eventually get rotated out to S3, so if you need to dig up really old
stuff, you'll need to look in our S3 log backup bucket.

6. Run the logs_to_sql.py script to load all of the JSON logs into a
SQLite database called aptrust_logs.db. That will appear in the db
directory.

7. From this directory, run the following to merge all of the SQLite
databases into a single database called aptrust.db:

```
sqlite3 db/aptrust.db < merge_dbs.sql
```

At this point, you will have all of the necessary raw audit tables
in aptrust.db, and they will be indexed for fast querying.

## Custom Audit Tables

You may need to build custom tables for your specific audit. If so,
take a look at build_audit_001_tables.sql for an example of how to
do that. For the first audit, the following command builds the
custom audit tables:

```
sqlite3 db/aptrust.db < build_audit_001_tables.sql
```

The script audit_001.py gleans information from those tables to
create a list of actions to fix errors uncovered by the audit.
