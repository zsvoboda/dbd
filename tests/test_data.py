import os
import json

from sqlalchemy import MetaData

from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.db.db_table import DbTable
from dbd.executors.model_executor import ModelExecutor


def compute_fingerprint(engine):
    meta = MetaData()
    meta.reflect(bind=engine)
    tables = ['test_typed', 'test_typed_1', 'test_typed_json', 'test_typed_json_1', 'test_typed_parquet',
              'test_typed_parquet_1', 'test_typed_excel', 'test_typed_excel_1']
    fingerprint = {}
    for table_name in tables:
        db_table = DbTable.from_alchemy_table(meta.tables[table_name])
        fingerprint[table_name] = db_table.fingerprint(engine)

    return fingerprint


def test_postgres():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/postgres/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/pgsql.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_mysql():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/mysql/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/mysql.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_sqlite():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/sqlite/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/sqlite.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_snowflake():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/snowflake/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/snowflake.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_bigquery():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/bigquery/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/bigquery.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_redshift():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/redshift/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/redshift.json', 'w') as fp:
        json.dump(fingerprint, fp)


def pytest_sessionfinish(session, exitstatus):
    fingerprints = {}
    for db in ['pgsql', 'mysql', 'sqlite', 'snowflake', 'bigquery', 'redshift']:
        fingerprints[db] = json.loads(open(f'tmp/{db}.json').read())
    for fingerprint in fingerprints.values():
        for other in fingerprints.values():
            assert fingerprint == other
