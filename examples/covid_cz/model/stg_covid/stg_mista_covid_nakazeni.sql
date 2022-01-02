select id,
       datum::date,
       vek::integer,
       pohlavi,
       kraj_nuts_kod,
       okres_lau_kod,
       nakaza_v_zahranici::integer,
       nakaza_zeme_csu_kod,
       reportovano_khs::integer
from is_covid.is_mista_covid_nakazeni