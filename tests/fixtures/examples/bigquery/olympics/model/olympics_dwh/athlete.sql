select distinct
    Name      as athlete_name,
    Sex       as athlete_sex,
    Age       as athlete_age,
    Height    as athlete_height,
    Weight    as athlete_weight
from olympics_stage.athlete_events
