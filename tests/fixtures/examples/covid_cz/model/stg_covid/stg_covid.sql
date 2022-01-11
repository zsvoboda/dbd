select datum::date,
       kumulativni_pocet_nakazenych::integer,
       kumulativni_pocet_vylecenych::integer,
       kumulativni_pocet_umrti::integer,
       kumulativni_pocet_testu::integer,
       kumulativni_pocet_ag_testu::integer,
       prirustkovy_pocet_nakazenych::integer,
       prirustkovy_pocet_vylecenych::integer,
       prirustkovy_pocet_umrti::integer,
       prirustkovy_pocet_provedenych_testu::integer,
       prirustkovy_pocet_provedenych_ag_testu::integer
from is_covid.is_covid