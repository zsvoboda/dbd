select distinct okres_kod::char(6) as district_id,
                kraj_kod::char(5)  as county_id,
                okres::varchar(50) as district_name
from stg_covid.stg_mista