import os.path

from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject


def test_basic_project():
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/basic/dbd.project')
    assert project.model_directory_from_project() == os.path.normpath('tests/fixtures/capabilities/basic/model')
    assert str(project.alchemy_engine_from_project().url) == profile.db_connections().get('basic').get('db.url')


def test_data_formats_project():
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/capabilities/data_formats/dbd.project')
    assert project.model_directory_from_project() == os.path.normpath('tests/fixtures/capabilities/data_formats/model')
    assert str(project.alchemy_engine_from_project().url) == profile.db_connections().get('data_formats').get('db.url')


def test_copy_stage_database():
    profile = DbdProfile.load('./tests/fixtures/databases/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/databases/redshift/dbd.project')
    copy_stage = project.copy_stage_from_project()
    assert 'url' in copy_stage
    assert 'access_key' in copy_stage
    assert 'secret_key' in copy_stage

def test_copy_stage_performance():
    profile = DbdProfile.load('./tests/fixtures/performance/dbd.profile.redshift')
    project = DbdProject.load(profile, 'tests/fixtures/performance/covid_czech/dbd.project')
    copy_stage = project.copy_stage_from_project()
    assert 'url' in copy_stage
    assert 'access_key' in copy_stage
    assert 'secret_key' in copy_stage
