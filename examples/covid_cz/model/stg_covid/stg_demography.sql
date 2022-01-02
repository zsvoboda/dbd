SELECT obec_kod,
       obec_kod2,
       obec,
       pocet_obyvatel::integer,
       pocet_muzi::integer,
       pocet_zeny::integer,
       vek_prumer::float,
       vek_prumer_zeny::float,
       vek_prumer_muzi::float
FROM is_covid.is_demografie_2021