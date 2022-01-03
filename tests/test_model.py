from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.db.db_schema import DbSchema
from dbd.executors.model_executor import ModelExecutor


def test_basic_model():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)

    schema = DbSchema.from_alchemy_engine(None, engine)

    assert len(schema.tables()) == 5
    table_names = [t.name() for t in schema.tables()]
    assert 'area' in table_names

    area_column_names = [c.name() for c in schema.table('area').columns()]

    assert 'state_name' in area_column_names
    assert str(schema.table('area').column(
        'state_name').alchemy_column().type) == "VARCHAR(50)"
    assert schema.table('area').column(
        'state_name').alchemy_column().primary_key
    assert not schema.table('area').column(
        'state_name').alchemy_column().nullable

    assert 'area_sq_mi' in area_column_names
    assert str(schema.table('area').column(
        'area_sq_mi').alchemy_column().type) == "INTEGER"
    assert not schema.table('area').column(
        'area_sq_mi').alchemy_column().nullable

    assert len(schema.table('area').alchemy_table().constraints) == 2
    assert len(schema.table('area').alchemy_table().indexes) == 0
    assert len(schema.table('area').alchemy_table().foreign_keys) == 1

    assert 'population' in table_names

    population_column_names = [c.name() for c in schema.table('population').columns()]

    assert 'state_code' in population_column_names
    assert str(schema.table('population').column(
        'state_code').alchemy_column().type) == "CHAR(2)"
    assert not schema.table('population').column(
        'state_code').alchemy_column().nullable

    assert 'ages' in population_column_names
    assert str(schema.table('population').column(
        'ages').alchemy_column().type) == "CHAR(7)"
    assert not schema.table('population').column(
        'ages').alchemy_column().nullable

    assert 'year' in population_column_names
    assert str(schema.table('population').column(
        'year').alchemy_column().type) == "INTEGER"
    assert not schema.table('population').column(
        'year').alchemy_column().nullable

    assert 'population' in population_column_names
    assert str(schema.table('population').column(
        'population').alchemy_column().type) == "INTEGER"

    assert len(schema.table('population').alchemy_table().constraints) == 2
    assert len(schema.table('population').alchemy_table().indexes) == 2
    assert len(schema.table('population').alchemy_table().foreign_keys) == 1

    assert 'state' in table_names

    state_column_names = [c.name() for c in schema.table('state').columns()]

    assert 'state_name' in state_column_names
    assert str(schema.table('state').column(
        'state_name').alchemy_column().type) == "VARCHAR(50)"
    assert not schema.table('state').column(
        'state_name').alchemy_column().nullable

    assert 'state_code' in state_column_names
    assert str(schema.table('state').column(
        'state_code').alchemy_column().type) == "CHAR(2)"
    assert schema.table('state').column(
        'state_code').alchemy_column().primary_key
    assert not schema.table('state').column(
        'state_code').alchemy_column().nullable

    assert len(schema.table('state').alchemy_table().constraints) == 2
    assert len(schema.table('state').alchemy_table().indexes) == 0
    assert len(schema.table('state').alchemy_table().foreign_keys) == 0

    assert 'total_population_2k' in table_names

    total_column_names = [c.name() for c in schema.table('total_population_2k').columns()]

    assert 'state_code' in total_column_names
    assert str(schema.table('total_population_2k').column(
        'state_code').alchemy_column().type) == "CHAR(2)"
    assert schema.table('total_population_2k').column(
        'state_code').alchemy_column().primary_key
    assert not schema.table('total_population_2k').column(
        'state_code').alchemy_column().nullable

    for i in range(14):
        column_name = f"total_20{str(i).rjust(2, '0')}"
        assert column_name in total_column_names
        assert str(schema.table('total_population_2k').column(
            column_name).alchemy_column().type) == "INTEGER"
        assert not schema.table('total_population_2k').column(
            column_name).alchemy_column().nullable

    assert len(schema.table('total_population_2k').alchemy_table().constraints) == 2
    assert len(schema.table('total_population_2k').alchemy_table().indexes) == 0
    assert len(schema.table('total_population_2k').alchemy_table().foreign_keys) == 1

    assert 'under18_population_2k' in table_names

    under_column_names = [c.name() for c in schema.table('under18_population_2k').columns()]

    assert 'state_code' in under_column_names
    assert str(schema.table('under18_population_2k').column(
        'state_code').alchemy_column().type) == "CHAR(2)"
    assert schema.table('under18_population_2k').column(
        'state_code').alchemy_column().primary_key
    assert not schema.table('under18_population_2k').column(
        'state_code').alchemy_column().nullable

    for i in range(14):
        column_name = f"under18_20{str(i).rjust(2, '0')}"
        assert column_name in under_column_names
        assert str(schema.table('under18_population_2k').column(
            column_name).alchemy_column().type) == "INTEGER"
        assert not schema.table('under18_population_2k').column(
            column_name).alchemy_column().nullable

    assert len(schema.table('under18_population_2k').alchemy_table().constraints) == 2
    assert len(schema.table('under18_population_2k').alchemy_table().indexes) == 0
    assert len(schema.table('under18_population_2k').alchemy_table().foreign_keys) == 1


def test_covid_cz():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/covid_cz/dbd.project')
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


def test_validate_basic():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/basic/dbd.project')
    model = ModelExecutor(project)
    validation_result, validation_errors = model.validate()
    assert validation_result


def test_validate_covid():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/covid_cz/dbd.project')
    model = ModelExecutor(project)
    validation_result, validation_errors = model.validate()
    assert validation_result
