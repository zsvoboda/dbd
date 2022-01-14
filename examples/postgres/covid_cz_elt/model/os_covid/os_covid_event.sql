select (row_number() over ())::integer                                                    as covid_event_id,
       datum::DATE                                                                        as covid_event_date,
       covid_event_type::char(1),
       vek::char(3)                                                                       as covid_event_person_age,
       lpad((vek::integer)::char(3), 3, '0')::char(3)                                     as covid_event_person_age_padded,
       (case when pohlavi = 'Z' then 'F' when pohlavi = 'M' then 'M' end)::char(1)          as covid_event_person_gender,
       okres_lau_kod::char(6)                                                             as district_id,
       1::smallint                                                                        as covid_event_cnt
from (select id, datum, 'I' as covid_event_type, vek::integer, pohlavi, okres_lau_kod from stg_covid.stg_mista_covid_nakazeni
      UNION
      select id, datum, 'R' as covid_event_type, vek::integer, pohlavi, okres_lau_kod from stg_covid.stg_mista_covid_vyleceni
      UNION
      select id, datum, 'D' as covid_event_type, vek::integer, pohlavi, okres_lau_kod from stg_covid.stg_mista_covid_umrti) a
