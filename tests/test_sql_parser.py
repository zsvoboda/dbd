from dbd.utils.sql_parser import SqlParser, SQlParserException


def test_simple_sql():
    tables = SqlParser.extract_tables("SELECT * FROM os_covid.os_city")
    assert tables[0] == "os_covid.os_city"


def test_join_sql():
    tables = SqlParser.extract_tables("SELECT * FROM os_covid.os_city AS c "
                                      "JOIN os_covid.os_demography AS d ON d.city_id = c.city_id")
    assert tables == ["os_covid.os_city", "os_covid.os_demography"]


def test_cte_sql():
    tables = SqlParser.extract_tables("with cities as (select city_id, district_id, city_name from os_covid.os_city), "
                                      "districts as (select district_id, district_name from os_covid.os_district) "
                                      "select d.district_name, c.city_name from cities c "
                                      "join districts d on c.district_id = d.district_id")
    assert tables == ["os_covid.os_city", "os_covid.os_district"]


def test_foreign_key_tables():
    tables = SqlParser.extract_foreign_key_tables(["os_city.city_id"])
    assert tables == ["os_city"]
    tables = SqlParser.extract_foreign_key_tables(["os_city.city_id", "os_district.district_id"])
    assert tables == ["os_city", "os_district"]
    try:
        SqlParser.extract_foreign_key_tables(["os_city.city_id", "district_id"])
        assert False
    except SQlParserException:
        assert True


def test_datatype_parser():
    assert str(SqlParser.parse_alchemy_data_type("VARCHAR(20)")) == "VARCHAR(20)"
    assert str(SqlParser.parse_alchemy_data_type("CHAR(5)")) == "CHAR(5)"
    assert str(SqlParser.parse_alchemy_data_type("DECIMAL(13,2)")) == "DECIMAL(13, 2)"
    assert str(SqlParser.parse_alchemy_data_type("TIMESTAMP")) == "TIMESTAMP"
    assert str(SqlParser.parse_alchemy_data_type("DATE")) == "DATE"
    assert str(SqlParser.parse_alchemy_data_type("TEXT")) == "TEXT"


def test_remove_comments():
    sql_text = """
-- Postgres foreign data wrapper     
drop extension if exists file_fdw cascade;
create extension file_fdw;

/*
    FDW server
    Doesn't work on GCP 
*/
drop server if exists covid_source_data cascade;
create server covid_source_data foreign data wrapper file_fdw;

-- Geografie

drop foreign table if exists is_covid.is_mista;
create foreign table is_covid.is_mista
(
	obec text,
	obec_kod text,
	okres text,
	okres_kod text, -- all columns as text datatype
	kraj text,
	kraj_kod text,
	psc text,
	latitude text,
	longitude text
)server covid_source_data options
(
    program 'curl -s https://raw.githubusercontent.com/33bcdd/souradnice-mest/master/souradnice.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
);

SELECT '--this isnt comment' FROM os_city;
SELECT '/* and this one too */' FROM \"os_city--test\";
"""
    desired_result = """drop extension if exists file_fdw cascade;
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
)server covid_source_data options
(
    program 'curl -s https://raw.githubusercontent.com/33bcdd/souradnice-mest/master/souradnice.csv || exit $(( $? == 23 ? 0 : $? ))',
    format 'csv',
    header 'true'
);
SELECT '--this isnt comment' FROM os_city;
SELECT '/* and this one too */' FROM \"os_city--test\";"""

    assert SqlParser.remove_sql_comments(sql_text) == desired_result
