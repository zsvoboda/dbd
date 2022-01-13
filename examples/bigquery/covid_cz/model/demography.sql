select distinct obec_kod2           as demography_id,
                obec_kod2           as city_id,
                pocet_obyvatel      as city_population,
                pocet_muzi          as city_population_male,
                pocet_zeny          as city_population_female,
                vek_prumer          as city_average_age,
                vek_prumer_muzi     as city_average_age_male,
                vek_prumer_zeny     as city_average_age_female
from ext_demografie_2021
