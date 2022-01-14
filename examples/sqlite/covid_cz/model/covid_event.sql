select (row_number() over ())                                                       as covid_event_id,
       datum                                                                        as covid_event_date,
       covid_event_type,
       cast(vek as CHAR(3))                                                         as covid_event_person_age,
       vek                                                                          as covid_event_person_age_num,
       (case when pohlavi = 'Z' then 'F' when pohlavi = 'M' then 'M' end)           as covid_event_person_gender,
       okres_lau_kod                                                                as district_id,
       1                                                                            as covid_event_cnt
from (select id, datum, 'I' as covid_event_type, vek, pohlavi, okres_lau_kod from ext_osoby
      UNION
      select id, datum, 'R' as covid_event_type, vek, pohlavi, okres_lau_kod from ext_vyleceni
      UNION
      select id, datum, 'D' as covid_event_type, vek, pohlavi, okres_lau_kod from ext_umrti) a
