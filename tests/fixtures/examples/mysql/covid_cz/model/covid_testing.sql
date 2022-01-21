select (row_number() over ())                 as covid_testing_id,
       'CZ'                                   as country_id,
       datum                                  as covid_testing_date,
       prirustkovy_pocet_provedenych_ag_testu as covid_testing_type_ag,
       prirustkovy_pocet_provedenych_testu    as covid_testing_type_pcr
from ext_nakazeni_vyleceni_umrti_testy
