select distinct obec_kod    as city_id,
                okres_kod   as district_id,
                obec        as city_name,
                latitude    as city_latitude,
                longitude   as city_longitude
from ext_souradnice
