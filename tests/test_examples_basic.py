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
    tables = ['area', 'population', 'state', 'us_states']
    fingerprint = {}
    for table_name in tables:
        db_table = DbTable.from_alchemy_table(meta.tables[table_name])
        fingerprint[table_name] = db_table.fingerprint(engine)

    return fingerprint


def test_postgres():
    profile = DbdProfile.load('./tests/fixtures/examples/postgres/basic/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/postgres/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/pgsql.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_mysql():
    profile = DbdProfile.load('./tests/fixtures/examples/mysql/basic/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/mysql/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/mysql.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_sqlite():
    profile = DbdProfile.load('./tests/fixtures/examples/sqlite/basic/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/sqlite/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/sqlite.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_snowflake():
    profile = DbdProfile.load('./tests/fixtures/examples/snowflake/basic/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/snowflake/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/snowflake.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_bigquery():
    profile = DbdProfile.load('./tests/fixtures/examples/bigquery/basic/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/bigquery/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/bigquery.json', 'w') as fp:
        json.dump(fingerprint, fp)


def test_redshift():
    profile = DbdProfile.load('./tests/fixtures/examples/redshift/basic/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/redshift/basic/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)
    fingerprint = compute_fingerprint(engine)
    with open('tmp/redshift.json', 'w') as fp:
        json.dump(fingerprint, fp)

def pytest_sessionfinish(session, exitstatus):
    print("Comparing results.")
    fingerprints = {}
    for db in ['pgsql', 'mysql', 'sqlite', 'snowflake', 'bigquery', 'redshift']:
        fingerprints[db] = json.loads(open(f'tmp/{db}.json').read())
    for fingerprint in fingerprints.values():
        for other in fingerprints.values():
            assert fingerprint == other
