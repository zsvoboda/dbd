drop extension if exists file_fdw cascade;
create extension file_fdw;

drop server if exists covid_source_data cascade;
create server covid_source_data foreign data wrapper file_fdw;

drop foreign table if exists is_covid.is_mista;
create foreign table is_covid.is_mista
    (
        obec text,
        obec_kod text,
        okres text,
        okres_kod text,
        kraj text,
        kraj_kod text,
        psc text,
        latitude text,
        longitude text
        )
    server covid_source_data options
    (
    program 'curl -s https://raw.githubusercontent.com/33bcdd/souradnice-mest/master/souradnice.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_mista_covid;
create foreign table is_covid.is_mista_covid
    (
        id text,
        den text,
        datum text,
        kraj_kod text,
        kraj_nazev text,
        okres_kod text,
        okres_nazev text,
        orp_kod text,
        orp_nazev text,
        obec_kod text,
        obec_nazev text,
        nove_pripady text,
        aktivni_pripady text,
        nove_pripady_65 text,
        nove_pripady_7_dni text,
        nove_pripady_14_dni text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/obce.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );
drop foreign table if exists is_covid.is_mista_covid_kumul;
create foreign table is_covid.is_mista_covid_kumul
    (
        id text,
        datum text,
        kraj_kod text,
        okres_kod text,
        kumulativni_pocet_nakazenych text,
        kumulativni_pocet_vylecenych text,
        kumulativni_pocet_umrti text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-nakazeni-vyleceni-umrti.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_mista_covid_orp;
create foreign table is_covid.is_mista_covid_orp
    (
        id text,
        den text,
        datum text,
        orp_kod text,
        orp_nazev text,
        incidence_7 text,
        incidence_65_7 text,
        incidence_75_7 text,
        prevalence text,
        prevalence_65 text,
        prevalence_75 text,
        aktualni_pocet_hospitalizovanych_osob text,
        nove_hosp_7 text,
        testy_7 text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/orp.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_covid_hospitalizace;
create foreign table is_covid.is_covid_hospitalizace
    (
        id text,
        datum text,
        pacient_prvni_zaznam text,
        kum_pacient_prvni_zaznam text,
        pocet_hosp text,
        stav_bez_priznaku text,
        stav_lehky text,
        stav_stredni text,
        stav_tezky text,
        jip text,
        kyslik text,
        hfno text,
        upv text,
        ecmo text,
        tezky_upv_ecmo text,
        umrti text,
        kum_umrti text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_covid;
create foreign table is_covid.is_covid
    (
        datum text,
        kumulativni_pocet_nakazenych text,
        kumulativni_pocet_vylecenych text,
        kumulativni_pocet_umrti text,
        kumulativni_pocet_testu text,
        kumulativni_pocet_ag_testu text,
        prirustkovy_pocet_nakazenych text,
        prirustkovy_pocet_vylecenych text,
        prirustkovy_pocet_umrti text,
        prirustkovy_pocet_provedenych_testu text,
        prirustkovy_pocet_provedenych_ag_testu text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/nakazeni-vyleceni-umrti-testy.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_mista_covid_nakazeni;
create foreign table is_covid.is_mista_covid_nakazeni
    (
        id text,
        datum text,
        vek text,
        pohlavi text,
        kraj_nuts_kod text,
        okres_lau_kod text,
        nakaza_v_zahranici text,
        nakaza_zeme_csu_kod text,
        reportovano_khs text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_mista_covid_vyleceni;
create foreign table is_covid.is_mista_covid_vyleceni
    (
        id text,
        datum text,
        vek text,
        pohlavi text,
        kraj_nuts_kod text,
        okres_lau_kod text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/vyleceni.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );

drop foreign table if exists is_covid.is_mista_covid_umrti;
create foreign table is_covid.is_mista_covid_umrti
    (
        id text,
        datum text,
        vek text,
        pohlavi text,
        kraj_nuts_kod text,
        okres_lau_kod text
        )
    server covid_source_data options
    (
    program 'curl -s https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/umrti.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
    );