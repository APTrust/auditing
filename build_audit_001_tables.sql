-- build_audit_001_tables.sql
--
-- Build the SQL tables that will let us start the audit.
--
-- audit_001_objects contains information about intellectual
-- objects, and audit_001_files contains information about
-- the files that compose those objects.
--
-- Usage:
--
-- sqlite3 db/aptrust.db < build_audit_001_tables.sql
--

create table audit_001_objects (
  bucket varchar(80),
  key varchar(80),
  institution_id int,
  object_id int,
  fedora_object_pid varchar(40),
  object_identifier varchar(255),
  ingest_record_id int,
  tar_result_id int,
  bag_read_result_id int,
  error_message text,
  stage varchar(40),
  retry boolean,
  identifier_assignment_count int not null default 0,
  ingest_count int not null default 0,
  fixity_check_count int not null default 0,
  fixity_generation_count int not null default 0,
  access_assignment_count int not null default 0
);

insert into audit_001_objects(
  bucket,
  key,
  institution_id,
  object_id,
  fedora_object_pid,
  object_identifier,
  ingest_record_id,
  tar_result_id,
  bag_read_result_id,
  error_message,
  stage,
  retry,
  identifier_assignment_count,
  ingest_count,
  fixity_check_count,
  fixity_generation_count,
  access_assignment_count)
select wi.bucket,        -- S3 receiving bucket
       wi.name,          -- name of tar file
       o.institution_id,
       o.id as object_id,
       o.pid as fedora_object_pid,
       o.identifier as object_identifier,
       ir.id as ingest_record_id,
       tr.id as tar_result_id,
       br.id as bag_read_result_id,
       ir.error_message,
       ir.stage,
       ir.retry,
       0,
       0,
       0,
       0,
       0
       from work_items wi
       inner join objects o on o.bag_name = rtrim(wi.name, '.tar')
       inner join ingest_records ir on ir.object_identifier = o.identifier || '.tar'
       inner join ingest_tar_results tr on tr.ingest_record_id = ir.id
       inner join ingest_bag_read_results br on br.ingest_record_id = ir.id
       where wi.stage='Record' and wi.status='Failed';


create table audit_001_files (
  ingest_record_id int,
  unpacked_file_path varchar(255),
  gf_id int,
  gf_file_path varchar(255),
  gf_uuid varchar(255),
  gf_identifier varchar(255),
  gf_needs_save boolean,
  gf_storage_url varchar(255),
  gf_stored_at varchar(255),
  gf_replication_error text,
  fedora_file_id int,
  fedora_file_pid varchar(40),
  fedora_file_uri varchar(255),
  s3_keys_id_s3 int,
  s3_key varchar(40),
  s3_keys_id_glacier int,
  glacier_key varchar(40),
  identifier_assignment_count int not null default 0,
  ingest_count int not null default 0,
  fixity_check_count int not null default 0,
  fixity_generation_count int not null default 0,
  access_assignment_count int not null default 0
);


insert into audit_001_files (
  ingest_record_id,
  unpacked_file_path,
  gf_id,
  gf_file_path,
  gf_uuid,
  gf_identifier,
  gf_needs_save,
  gf_storage_url,
  gf_stored_at,
  gf_replication_error,
  fedora_file_id,
  fedora_file_pid,
  fedora_file_uri,
  s3_keys_id_s3,
  s3_key,
  s3_keys_id_glacier,
  glacier_key,
  identifier_assignment_count,
  ingest_count,
  fixity_check_count,
  fixity_generation_count,
  access_assignment_count
)
select o.ingest_record_id,
       uf.file_path,
       igf.id,
       igf.file_path,
       igf.uuid,
       igf.identifier,
       igf.needs_save,
       igf.storage_url,
       igf.stored_at,
       igf.replication_error,
       f.id,
       f.pid,
       f.uri,
       s1.id as s3_keys_id_s3,
       s1.name as s3_key,
       s2.id as s3_keys_id_glacier,
       s2.name as glacier_key,
       0,
       0,
       0,
       0,
       0
from ingest_unpacked_files uf
inner join audit_001_objects o on o.tar_result_id = uf.ingest_tar_result_id
left join ingest_generic_files igf on igf.ingest_tar_result_id = uf.ingest_tar_result_id and igf.file_path = uf.file_path
left join s3_keys s1 on (s1.name = igf.uuid or s1.name = substr(igf.storage_url, 55)) and s1.bucket = 'aptrust.preservation.storage'
left join s3_keys s2 on (s2.name = igf.uuid or s2.name = substr(igf.storage_url, 55)) and s2.bucket = 'aptrust.preservation.oregon'
left join files f on f.identifier = o.object_identifier || '/' || uf.file_path
where uf.file_path like 'data/%';


/*

Count queries to start audit

select count(*) from audit_001_files;
select count(distinct(gf_identifier)) from audit_001_files;

select count(*) from ingest_unpacked_files uf
inner join audit_001_objects o on o.tar_result_id = uf.ingest_tar_result_id
where uf.file_path like 'data/%';

*/
