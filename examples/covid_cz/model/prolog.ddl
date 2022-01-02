drop schema if exists is_covid cascade;
create schema if not exists is_covid;
alter schema is_covid owner to demouser;

drop schema if exists stg_covid cascade;
create schema if not exists stg_covid;
alter schema stg_covid owner to demouser;

drop schema if exists os_covid cascade;
create schema if not exists os_covid;
alter schema os_covid owner to demouser;
