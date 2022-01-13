from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.executors.model_executor import ModelExecutor


def validate_result(engine, table_name='test_typed', compensation=0, ):
    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert result.fetchall() == [(14 - compensation,)], f"fALL row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_date IS NULL")
        assert result.fetchall() == [(2 - compensation,)], f"test_date IS NULL row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_datetime IS NULL")
        assert result.fetchall() == [
            (3 - compensation,)], f"test_datetime IS NULL row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_bool IS NULL")
        assert result.fetchall() == [(5 - compensation,)], f"test_bool IS NULL row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_string IS NULL")
        assert result.fetchall() == [
            (3 - compensation,)], f"test_string IS NULL row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_int IS NULL")
        assert result.fetchall() == [(8 - compensation,)], f"test_int IS NULL row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_float IS NULL")
        assert result.fetchall() == [(4 - compensation,)], f"test_float IS NULL row count failed for table {table_name}"
        result = conn.execute(f"SELECT SUM(test_int) FROM {table_name}")
        assert result.fetchall() == [(6,)], f"SUM(test_int) test failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_float < 1")
        assert result.fetchall() == [(1,)], f"test_float < 1 row count failed for table {table_name}"
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE test_datetime > '2020-01-22'")
        assert result.fetchall() == [(10,)], f"test_datetime > 2020-01-22 row count failed for table {table_name}"
        result = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE test_date IS NULL AND test_datetime IS NULL AND "
            f"test_bool IS NULL AND test_string IS NULL AND test_int IS NULL AND test_float IS NULL")
        assert result.fetchall() == [(1 - compensation,)], f"ALL NULL row count failed for table {table_name}"

def test_bigquery():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/bigquery/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    validate_result(engine, table_name='test_typed')
    validate_result(engine, table_name='test_typed_json')
    validate_result(engine, table_name='test_typed_parquet')
    validate_result(engine, table_name='test_typed_excel', compensation=1)

"""
def test_snowflake():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/snowflake/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    validate_result(engine, table_name='test_typed'.upper())
    validate_result(engine, table_name='test_typed_json'.upper())
    validate_result(engine, table_name='test_typed_parquet'.upper())
    validate_result(engine, table_name='test_typed_excel'.upper(), compensation=1)


def test_redshift():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/redshift/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    validate_result(engine, table_name='test_typed')
    validate_result(engine, table_name='test_typed_json')
    validate_result(engine, table_name='test_typed_parquet')
    validate_result(engine, table_name='test_typed_excel', compensation=1)


def test_sqlite():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/sqlite/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    validate_result(engine, table_name='test_typed')
    validate_result(engine, table_name='test_typed_json')
    validate_result(engine, table_name='test_typed_parquet')
    validate_result(engine, table_name='test_typed_excel', compensation=1)


def test_postgres():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/postgres/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    validate_result(engine, table_name='test_typed')
    validate_result(engine, table_name='test_typed_json')
    validate_result(engine, table_name='test_typed_parquet')
    validate_result(engine, table_name='test_typed_excel', compensation=1)


def test_mysql():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/mysql/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    validate_result(engine, table_name='test_typed')
    validate_result(engine, table_name='test_typed_json')
    validate_result(engine, table_name='test_typed_parquet')
    validate_result(engine, table_name='test_typed_excel', compensation=1)
    
"""

