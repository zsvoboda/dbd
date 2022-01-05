select id,
       datum::date,
       kraj_kod,
       okres_kod,
       kumulativni_pocet_nakazenych::integer,
       kumulativni_pocet_vylecenych::integer,
       kumulativni_pocet_umrti::integer
from is_covid.is_mista_covid_kumul