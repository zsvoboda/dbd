select distinct obec_kod2::char(6)      as demography_id,
                obec_kod2::char(6)      as city_id,
                pocet_obyvatel::integer as city_population,
                pocet_muzi::integer     as city_population_male,
                pocet_zeny::integer     as city_population_female,
                vek_prumer::float       as city_average_age,
                vek_prumer_muzi::float  as city_average_age_male,
                vek_prumer_zeny::float  as city_average_age_female
from stg_covid.stg_demography