import os

from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.db.db_schema import DbSchema
from dbd.executors.model_executor import ModelExecutor, ModelExecutionException


def __delete_db_file(dbfile='./tmp/basic.db'):
    if os.path.exists(dbfile):
        os.remove(dbfile)


def test_zip_on_url():
    __delete_db_file('./tmp/zip_on_url.db')
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/zip_on_url/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)


def test_zip_local():
    __delete_db_file('./tmp/zip_local.db')
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/zip_local/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)


def test_ref_file():
    __delete_db_file('./tmp/ref_file.db')
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/ref_file/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)


def test_jinja():
    __delete_db_file('./tmp/jinja_template.db')
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/jinja_template/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)


def test_basic_model_selected():
    __delete_db_file()
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine, ['state'])


def test_basic_model_selected2():
    __delete_db_file()
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine, ['area', 'state'])


def test_basic_model_selected_with_deps():
    __delete_db_file()
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine, ['us_states'])


def test_basic_model_selected_without_deps():
    __delete_db_file()
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine, ['us_states'], False)
    except ModelExecutionException as e:
        assert True
    else:
        assert False


def test_basic_model():
    __delete_db_file()
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)

    schema = DbSchema.from_alchemy_engine(None, engine)

    assert len(schema.tables()) == 4
    table_names = [t.name() for t in schema.tables()]
    assert 'area' in table_names

    area_column_names = [c.name() for c in schema.table('area').columns()]

    assert 'state_name' in area_column_names
    assert str(schema.table('area').column(
        'state_name').alchemy_column().type) == "TEXT"

    assert 'area_sq_mi' in area_column_names
    assert str(schema.table('area').column(
        'area_sq_mi').alchemy_column().type) == "TEXT"

    assert 'population' in table_names

    population_column_names = [c.name() for c in schema.table('population').columns()]

    assert 'state' in population_column_names
    assert str(schema.table('population').column(
        'state').alchemy_column().type) == "TEXT"

    assert 'population' in population_column_names
    assert str(schema.table('population').column(
        'population').alchemy_column().type) == "TEXT"

    assert 'state' in table_names

    state_column_names = [c.name() for c in schema.table('state').columns()]

    assert 'state' in state_column_names
    assert str(schema.table('state').column(
        'state').alchemy_column().type) == "TEXT"

    assert 'abbrev' in state_column_names
    assert str(schema.table('state').column(
        'abbrev').alchemy_column().type) == "TEXT"

    assert 'us_states' in table_names

    us_states_column_names = [c.name() for c in schema.table('us_states').columns()]

    assert 'state_code' in us_states_column_names
    assert str(schema.table('us_states').column(
        'state_code').alchemy_column().type) == "CHAR(2)"
    assert schema.table('us_states').column(
        'state_code').alchemy_column().primary_key
    assert not schema.table('us_states').column(
        'state_code').alchemy_column().nullable

    assert 'state_name' in us_states_column_names
    assert str(schema.table('us_states').column(
        'state_name').alchemy_column().type) == "VARCHAR(50)"
    assert not schema.table('us_states').column(
        'state_name').alchemy_column().nullable

    assert 'state_population' in us_states_column_names
    assert str(schema.table('us_states').column(
        'state_population').alchemy_column().type) == "INTEGER"
    assert not schema.table('us_states').column(
        'state_population').alchemy_column().nullable

    assert 'state_area_sq_mi' in us_states_column_names
    assert str(schema.table('us_states').column(
        'state_area_sq_mi').alchemy_column().type) == "INTEGER"
    assert not schema.table('us_states').column(
        'state_area_sq_mi').alchemy_column().nullable


def test_data_formats_model():
    __delete_db_file('./tmp/data_formats.db')
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/data_formats/dbd.project')
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


def test_validate_basic():
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/data_formats/dbd.project')
    model = ModelExecutor(project)
    validation_result, validation_errors = model.validate()
    assert validation_result
