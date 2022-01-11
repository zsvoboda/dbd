from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.db.db_schema import DbSchema
from dbd.executors.model_executor import ModelExecutor


def test_covid_refs():
    profile = DbdProfile.load('./tests/fixtures/examples/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/examples/covid_ref/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)

    schema = DbSchema.from_alchemy_engine(None, engine)

    assert len(schema.tables()) == 1
    table_names = [t.name() for t in schema.tables()]
    assert 'covid_ref' in table_names

    covid_column_names = [c.name() for c in schema.table('covid_ref').columns()]

    assert 'FIPS' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'FIPS').alchemy_column().type) == "CHAR(4)"

    assert 'Admin2' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Admin2').alchemy_column().type) == "VARCHAR(50)"

    assert 'Province_State' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Province_State').alchemy_column().type) == "VARCHAR(50)"

    assert 'Country_Region' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Country_Region').alchemy_column().type) == "VARCHAR(50)"

    assert 'Last_Update' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Last_Update').alchemy_column().type) == "TIMESTAMP"

    assert 'Lat' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Lat').alchemy_column().type) == "VARCHAR(10)"

    assert 'Long_' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Long_').alchemy_column().type) == "VARCHAR(10)"

    assert 'Confirmed' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Confirmed').alchemy_column().type) == "INTEGER"

    assert 'Recovered' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Recovered').alchemy_column().type) == "INTEGER"

    assert 'Active' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Active').alchemy_column().type) == "INTEGER"

    assert 'Combined_Key' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Combined_Key').alchemy_column().type) == "VARCHAR(100)"

    assert 'Incident_Rate' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Incident_Rate').alchemy_column().type) == "FLOAT"

    assert 'Case_Fatality_Ratio' in covid_column_names
    assert str(schema.table('covid_ref').column(
        'Case_Fatality_Ratio').alchemy_column().type) == "FLOAT"


def test_covid_cz():
    profile = DbdProfile.load('./tests/fixtures/examples/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/examples/covid_cz/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)

    schema = DbSchema.from_alchemy_engine('os_covid', engine)

    assert len(schema.tables()) == 8
    table_names = [t.name() for t in schema.tables()]
    assert 'os_city' in table_names

    city_column_names = [c.name() for c in schema.table('os_city').columns()]

    assert 'city_id' in city_column_names
    assert str(schema.table('os_city').column(
        'city_id').alchemy_column().type) == "CHAR(6)"
    assert schema.table('os_city').column(
        'city_id').alchemy_column().primary_key
    assert not schema.table('os_city').column(
        'city_id').alchemy_column().nullable

    assert 'city_name' in city_column_names
    assert str(schema.table('os_city').column(
        'city_name').alchemy_column().type) == "VARCHAR(50)"
    assert not schema.table('os_city').column(
        'city_name').alchemy_column().nullable

    assert len(schema.table('os_city').alchemy_table().constraints) == 2
    assert len(schema.table('os_city').alchemy_table().indexes) == 1
    assert len(schema.table('os_city').alchemy_table().foreign_keys) == 1

    assert 'os_country' in table_names

    country_column_names = [c.name() for c in schema.table('os_country').columns()]

    assert 'country_id' in country_column_names
    assert str(schema.table('os_country').column(
        'country_id').alchemy_column().type) == "CHAR(3)"
    assert schema.table('os_country').column(
        'country_id').alchemy_column().primary_key
    assert not schema.table('os_country').column(
        'country_id').alchemy_column().nullable

    assert 'country_name' in country_column_names
    assert str(schema.table('os_country').column(
        'country_name').alchemy_column().type) == "VARCHAR(50)"

    assert len(schema.table('os_country').alchemy_table().constraints) == 1
    assert len(schema.table('os_country').alchemy_table().indexes) == 0
    assert len(schema.table('os_country').alchemy_table().foreign_keys) == 0

    assert 'os_county' in table_names

    county_column_names = [c.name() for c in schema.table('os_county').columns()]

    assert 'county_id' in county_column_names
    assert str(schema.table('os_county').column(
        'county_id').alchemy_column().type) == "CHAR(5)"
    assert schema.table('os_county').column(
        'county_id').alchemy_column().primary_key
    assert not schema.table('os_county').column(
        'county_id').alchemy_column().nullable

    assert 'county_name' in county_column_names
    assert str(schema.table('os_county').column(
        'county_name').alchemy_column().type) == "VARCHAR(50)"
    # TODO: DEBUG county name should be unique?
    assert not schema.table('os_county').column(
        'county_name').alchemy_column().nullable

    assert 'country_id' in county_column_names
    assert str(schema.table('os_county').column(
        'country_id').alchemy_column().type) == "CHAR(3)"
    assert not schema.table('os_county').column(
        'country_id').alchemy_column().nullable

    assert len(schema.table('os_county').alchemy_table().constraints) == 3
    assert len(schema.table('os_county').alchemy_table().indexes) == 1
    assert len(schema.table('os_county').alchemy_table().foreign_keys) == 1

    assert 'os_covid_event' in table_names

    event_column_names = [c.name() for c in schema.table('os_covid_event').columns()]

    assert 'covid_event_id' in event_column_names
    assert str(schema.table('os_covid_event').column(
        'covid_event_id').alchemy_column().type) == "INTEGER"
    assert schema.table('os_covid_event').column(
        'covid_event_id').alchemy_column().primary_key
    assert not schema.table('os_covid_event').column(
        'covid_event_id').alchemy_column().nullable

    assert 'covid_event_date' in event_column_names
    assert str(schema.table('os_covid_event').column(
        'covid_event_date').alchemy_column().type) == "DATE"
    assert not schema.table('os_covid_event').column(
        'covid_event_date').alchemy_column().nullable

    assert 'covid_event_type' in event_column_names
    assert str(schema.table('os_covid_event').column(
        'covid_event_type').alchemy_column().type) == "CHAR(1)"
    assert not schema.table('os_covid_event').column(
        'covid_event_type').alchemy_column().nullable

    assert 'district_id' in event_column_names
    assert str(schema.table('os_covid_event').column(
        'district_id').alchemy_column().type) == "CHAR(6)"

    assert 'covid_event_cnt' in event_column_names
    assert str(schema.table('os_covid_event').column(
        'covid_event_cnt').alchemy_column().type) == "SMALLINT"

    assert len(schema.table('os_covid_event').alchemy_table().constraints) == 4
    assert len(schema.table('os_covid_event').alchemy_table().indexes) == 3
    assert len(schema.table('os_covid_event').alchemy_table().foreign_keys) == 1

    assert 'os_covid_hospitalisation' in table_names

    hospitalisation_column_names = [c.name() for c in schema.table('os_covid_hospitalisation').columns()]

    assert 'covid_hospitalisation_id' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_id').alchemy_column().type) == "INTEGER"
    assert schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_id').alchemy_column().primary_key
    assert not schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_id').alchemy_column().nullable

    assert 'country_id' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'country_id').alchemy_column().type) == "CHAR(3)"

    assert 'covid_hospitalisation_date' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_date').alchemy_column().type) == "DATE"
    assert not schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_date').alchemy_column().nullable

    assert 'covid_hospitalisation_admissions' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_admissions').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_current' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_current').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_no_symptoms' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_no_symptoms').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_light_symptoms' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_light_symptoms').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_medium_symptoms' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_medium_symptoms').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_intensive_care' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_intensive_care').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_oxygen' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_oxygen').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_hfno' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_hfno').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_ventilation' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_ventilation').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_ecmo' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_ecmo').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_ecmo_ventilation' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_ecmo_ventilation').alchemy_column().type) == "INTEGER"

    assert 'covid_hospitalisation_deaths' in hospitalisation_column_names
    assert str(schema.table('os_covid_hospitalisation').column(
        'covid_hospitalisation_deaths').alchemy_column().type) == "INTEGER"

    assert len(schema.table('os_covid_hospitalisation').alchemy_table().constraints) == 2
    assert len(schema.table('os_covid_hospitalisation').alchemy_table().indexes) == 2
    assert len(schema.table('os_covid_hospitalisation').alchemy_table().foreign_keys) == 1

    assert 'os_covid_testing' in table_names

    testing_column_names = [c.name() for c in schema.table('os_covid_testing').columns()]

    assert 'covid_testing_id' in testing_column_names
    assert str(schema.table('os_covid_testing').column(
        'covid_testing_id').alchemy_column().type) == "INTEGER"
    assert schema.table('os_covid_testing').column(
        'covid_testing_id').alchemy_column().primary_key
    assert not schema.table('os_covid_testing').column(
        'covid_testing_id').alchemy_column().nullable

    assert 'country_id' in testing_column_names
    assert str(schema.table('os_covid_testing').column(
        'country_id').alchemy_column().type) == "CHAR(3)"

    assert 'covid_testing_date' in testing_column_names
    assert str(schema.table('os_covid_testing').column(
        'covid_testing_date').alchemy_column().type) == "DATE"

    assert 'covid_testing_type_ag' in testing_column_names
    assert str(schema.table('os_covid_testing').column(
        'covid_testing_type_ag').alchemy_column().type) == "INTEGER"

    assert 'covid_testing_type_pcr' in testing_column_names
    assert str(schema.table('os_covid_testing').column(
        'covid_testing_type_pcr').alchemy_column().type) == "INTEGER"

    assert len(schema.table('os_covid_testing').alchemy_table().constraints) == 2
    assert len(schema.table('os_covid_testing').alchemy_table().indexes) == 1
    assert len(schema.table('os_covid_testing').alchemy_table().foreign_keys) == 1

    assert 'os_demography' in table_names
    demography_column_names = [c.name() for c in schema.table('os_demography').columns()]

    assert 'demography_id' in demography_column_names
    assert str(schema.table('os_demography').column(
        'demography_id').alchemy_column().type) == "CHAR(6)"
    assert schema.table('os_demography').column(
        'demography_id').alchemy_column().primary_key
    assert not schema.table('os_demography').column(
        'demography_id').alchemy_column().nullable

    assert 'city_id' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_id').alchemy_column().type) == "CHAR(6)"
    assert not schema.table('os_demography').column(
        'city_id').alchemy_column().nullable

    assert 'city_population' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_population').alchemy_column().type) == "INTEGER"

    assert 'city_population_male' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_population_male').alchemy_column().type) == "INTEGER"

    assert 'city_population_female' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_population_female').alchemy_column().type) == "INTEGER"

    assert 'city_average_age' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_average_age').alchemy_column().type) == "DOUBLE_PRECISION"

    assert 'city_average_age_male' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_average_age_male').alchemy_column().type) == "DOUBLE_PRECISION"

    assert 'city_average_age_female' in demography_column_names
    assert str(schema.table('os_demography').column(
        'city_average_age_female').alchemy_column().type) == "DOUBLE_PRECISION"

    assert len(schema.table('os_demography').alchemy_table().constraints) == 2
    assert len(schema.table('os_demography').alchemy_table().indexes) == 1
    assert len(schema.table('os_demography').alchemy_table().foreign_keys) == 1

    assert 'os_district' in table_names
    district_column_names = [c.name() for c in schema.table('os_district').columns()]

    assert 'district_id' in district_column_names
    assert str(schema.table('os_district').column(
        'district_id').alchemy_column().type) == "CHAR(6)"
    assert schema.table('os_district').column(
        'district_id').alchemy_column().primary_key
    assert not schema.table('os_district').column(
        'district_id').alchemy_column().nullable

    assert 'county_id' in district_column_names
    assert str(schema.table('os_district').column(
        'county_id').alchemy_column().type) == "CHAR(5)"
    assert not schema.table('os_district').column(
        'county_id').alchemy_column().nullable

    assert 'district_name' in district_column_names
    assert str(schema.table('os_district').column(
        'district_name').alchemy_column().type) == "VARCHAR(50)"

    assert len(schema.table('os_district').alchemy_table().constraints) == 2
    assert len(schema.table('os_district').alchemy_table().indexes) == 1
    assert len(schema.table('os_district').alchemy_table().foreign_keys) == 1


def test_validate_covid():
    profile = DbdProfile.load('./tests/fixtures/examples/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/examples/covid_cz/dbd.project')
    model = ModelExecutor(project)
    validation_result, validation_errors = model.validate()
    assert validation_result
