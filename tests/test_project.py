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
