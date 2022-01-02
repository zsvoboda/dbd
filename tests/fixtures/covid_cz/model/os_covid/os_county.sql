select distinct kraj_kod::char(5) as county_id, kraj::varchar(50) as county_name, 'CZ'::char(3) as country_id
from stg_covid.stg_mista