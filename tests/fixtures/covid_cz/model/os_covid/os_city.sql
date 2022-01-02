select distinct obec_kod::char(6)      as city_id,
                okres_kod::char(6)     as district_id,
                obec::varchar(50)      as city_name,
                latitude::varchar(10)  as city_latitude,
                longitude::varchar(10) as city_longitude
from stg_covid.stg_mista