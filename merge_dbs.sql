--
-- merge_dbs.sql
--
-- Merges the fedore, log and S3 databases into a single db.
-- This script assumes it's being run from the main audit directory,
-- and that the databases are in the db directory. To run:
--
-- sqlite3 db/aptrust.db < merge_dbs.sql
--
--
-- TODO: Table and index definitions were copied from the python
-- scripts. They should be moved to separate SQL files so this and
-- the python scripts can reference them from a single source.
--

------------------------------------------------------------------------
-- Fedora records
------------------------------------------------------------------------
attach 'db/aptrust_fedora.db' as fedora;

--
-- Create the schema with proper foreign keys.
--
create table institutions(
  id integer primary key autoincrement,
  pid varchar(40),
  name varchar(255),
  brief_name varchar(40),
  identifier varchar(80),
  dpn_uuid varchar(40));

create table users (
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


create table work_items(
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
  REFERENCES institutions(id));

create table objects(
  id integer primary key autoincrement,
  institution_id int not null,
  pid varchar(40),
  title varchar(255),
  description text,
  access varchar(40),
  bag_name varchar(255),
  identifier varchar(255),
  state char(1),
  alt_identifier varchar(255),
  FOREIGN KEY(institution_id) REFERENCES institutions(id));

create table files(
  id integer primary key autoincrement,
  object_id int,
  pid varchar(40),
  uri varchar(255),
  size unsigned big int,
  created datetime,
  modified datetime,
  file_format varchar(80),
  identifier varchar(255),
  state char(1),
  FOREIGN KEY(object_id) REFERENCES objects(id));

create table checksums(
  id integer primary key autoincrement,
  file_id int,
  algorithm varchar(10),
  digest varchar(80),
  date_time datetime,
  FOREIGN KEY(file_id) REFERENCES files(id));

create table events(
  id integer primary key autoincrement,
  object_id int null,
  file_id int null,
  identifier varchar(40),
  type varchar(80),
  date_time datetime,
  detail varchar(255),
  outcome varchar(255),
  outcome_detail varchar(255),
  object varchar(255),
  agent varchar(255),
  outcome_information varchar(255),
  FOREIGN KEY(object_id) REFERENCES objects(id)
  FOREIGN KEY(file_id) REFERENCES files(id));


--
-- Import the data
--
insert into institutions select * from fedora.institutions;
insert into users select * from fedora.users;
insert into objects select * from fedora.objects;
insert into files select * from fedora.files;
insert into checksums select * from fedora.checksums;
insert into events select * from fedora.events;
insert into work_items select * from fedora.work_items;

--
-- Create Indexes last, so they don't slow the inserts
--
create unique index ix_obj_pid on objects(pid);
create unique index ix_obj_identifier on objects(identifier);
create unique index ix_file_pid on files(pid);
create unique index ix_file_identifier on files(identifier);
create index ix_file_object_id on files(object_id);
create index ix_checksum_file_id on checksums(file_id);
create index ix_events_object_id on events(object_id);
create index ix_events_file_id on events(file_id);
create unique index ix_events_identifier on events(identifier);
create unique index ix_users_email on users(email);
create index ix_name_etag_bucket on work_items(name, etag, bucket);


------------------------------------------------------------------------
-- Log records
------------------------------------------------------------------------
attach 'db/aptrust_logs.db' as logs;

create table ingest_records(
  id integer primary key autoincrement,
  error_message text,
  stage varchar(40),
  retry bool,
  object_identifier varchar(255),
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp);

create table ingest_s3_files(
  id integer primary key autoincrement,
  ingest_record_id int not null,
  bucket_name varchar(255),
  key varchar(255),
  size int,
  etag varchar(80),
  last_modified datetime,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id));

create table ingest_fetch_results(
  id integer primary key autoincrement,
  ingest_record_id int not null,
  local_file varchar(255),
  remote_md5 varchar(80),
  local_md5 varchar(80),
  md5_verified bool,
  md5_verifiable bool,
  error_message text,
  warning text,
  retry bool,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id));

create table ingest_tar_results(
  id integer primary key autoincrement,
  ingest_record_id int not null,
  input_file varchar(255),
  output_dir varchar(255),
  error_message text,
  warnings text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id));

create table ingest_unpacked_files(
  id integer primary key autoincrement,
  ingest_tar_result_id int not null,
  file_path varchar(255),
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_tar_result_id)
  REFERENCES ingest_tar_results(id));

create table ingest_generic_files(
  id integer primary key autoincrement,
  ingest_tar_result_id int not null,
  file_path varchar(255),
  size int,
  file_created datetime,
  file_modified datetime,
  md5 varchar(80),
  md5_verified bool,
  sha256 varchar(80),
  sha256_generated datetime,
  uuid varchar(40),
  uuid_generated datetime,
  mime_type varchar(80),
  error_message text,
  storage_url varchar(255),
  stored_at datetime,
  storage_md5 varchar(80),
  identifier varchar(255),
  identifier_assigned datetime,
  existing_file bool,
  needs_save bool,
  replication_error text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_tar_result_id)
  REFERENCES ingest_tar_results(id));

create table ingest_bag_read_results(
  id integer primary key autoincrement,
  ingest_record_id int not null,
  bag_path varchar(255),
  error_message text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id));

create table ingest_bag_read_files(
  id integer primary key autoincrement,
  ingest_bag_read_result_id int not null,
  file_path varchar(255),
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_bag_read_result_id)
  REFERENCES ingest_bag_read_results(id));

create table ingest_checksum_errors(
  id integer primary key autoincrement,
  ingest_bag_read_result_id int not null,
  error_message text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_bag_read_result_id)
  REFERENCES ingest_bag_read_results(id));

create table ingest_tags(
  id integer primary key autoincrement,
  ingest_bag_read_result_id int not null,
  label varchar(255),
  value text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_bag_read_result_id)
  REFERENCES ingest_bag_read_results(id));

create table ingest_fedora_results(
  id integer primary key autoincrement,
  ingest_record_id int not null,
  object_identifier varchar(255),
  is_new_object bool,
  error_message text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_record_id) REFERENCES ingest_records(id));

create table ingest_fedora_generic_files(
  id integer primary key autoincrement,
  ingest_fedora_result_id int not null,
  file_path varchar(255),
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_fedora_result_id)
  REFERENCES ingest_fedora_results(id));

create table ingest_fedora_metadata(
  id integer primary key autoincrement,
  ingest_fedora_result_id int not null,
  record_type varchar(40),
  action varchar(40),
  event_object varchar(40),
  error_message text,
  created_at datetime default current_timestamp,
  updated_at datetime default current_timestamp,
  FOREIGN KEY(ingest_fedora_result_id)
  REFERENCES ingest_fedora_results(id));

--
-- Insert the data
--
insert into ingest_records select * from logs.ingest_records;
insert into ingest_S3_files select * from logs.ingest_s3_files;
insert into ingest_fetch_results select * from logs.ingest_fetch_results;
insert into ingest_tar_results select * from logs.ingest_tar_results;
insert into ingest_unpacked_files select * from logs.ingest_unpacked_files;
insert into ingest_generic_files select * from logs.ingest_generic_files;
insert into ingest_bag_read_results select * from logs.ingest_bag_read_results;
insert into ingest_bag_read_files select * from logs.ingest_bag_read_files;
insert into ingest_checksum_errors select * from logs.ingest_checksum_errors;
insert into ingest_tags select * from logs.ingest_tags;
insert into ingest_fedora_results select * from logs.ingest_fedora_results;
insert into ingest_fedora_generic_files select * from logs.ingest_fedora_generic_files;
insert into ingest_fedora_metadata select * from logs.ingest_fedora_metadata;

--
-- Create indexes
--
create index ix_ingest_ingest_etag_bucket_key_date
on ingest_s3_files(etag, bucket_name, key, last_modified);

create index ix_ingest_key
on ingest_s3_files(key);

create index ix_ingest_obj_identifier
on ingest_records(object_identifier);

create index ix_ingest_ingest_fetch_results_fk1
on ingest_fetch_results(ingest_record_id);

create index ix_ingest_ingest_tar_results_fk1
on ingest_tar_results(ingest_record_id);

create index ix_ingest_ingest_unpacked_files_fk1
on ingest_unpacked_files(ingest_tar_result_id);

create index ix_ingest_ingest_generic_files_fk1
on ingest_generic_files(ingest_tar_result_id);

create index ix_ingest_ingest_bag_read_results_fk1
on ingest_bag_read_results(ingest_record_id);

create index ix_ingest_ingest_bag_read_files_fk1
on ingest_bag_read_files(ingest_bag_read_result_id);

create index ix_ingest_ingest_checksum_errors_fk1
on ingest_checksum_errors(ingest_bag_read_result_id);

create index ix_ingest_ingest_tags_fk1
on ingest_tags(ingest_bag_read_result_id);

create index ix_ingest_fedora_results_fk1
on ingest_fedora_results(ingest_record_id);

create index ix_ingest_fedora_generic_files_fk1
on ingest_fedora_generic_files(ingest_fedora_result_id);

create index ix_ingest_fedora_metadata_fk1
on ingest_fedora_metadata(ingest_fedora_result_id);


------------------------------------------------------------------------
-- S3 records
------------------------------------------------------------------------
attach 'db/aptrust_s3.db' as s3;

create table s3_keys(
  id integer primary key autoincrement,
  bucket varchar(255),
  name varchar(255),
  cache_control varchar(40),
  content_type varchar(80),
  etag varchar(80),
  last_modified datetime,
  storage_class varchar(40),
  size int);

create table s3_meta(
  key_id integer,
  name varchar(255),
  value varchar(255));

--
-- Import data
--

insert into s3_keys select * from s3.s3_keys;
insert into s3_meta select * from s3.s3_meta;


--
-- Create Indexes
--

create unique index ix_s3_name_etag_bucket on s3_keys(name, etag, bucket);
create index ix_s3_meta_key_id on s3_meta(key_id);
create index ix_s3_meta_name_value on s3_meta(name, value);
