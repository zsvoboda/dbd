select distinct
    (row_number() over ())          as athlete_id,
    "Name"::varchar(120)            as athlete_name,
    "Sex"::char(1)                  as athlete_sex,
    lpad("Age"::char(3),3,'0')      as athlete_age,
    "Height"::integer               as athlete_height,
    "Weight"::float                 as athlete_weight
from "olympics_stage"."athlete_events"
