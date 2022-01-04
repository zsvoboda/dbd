import os.path

from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject


def test_basic_project():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/basic/dbd.project')
    assert project.model_directory_from_project() == os.path.normpath('tests/fixtures/basic/model')
    assert str(project.alchemy_engine_from_project().url) == profile.db_connections().get('states').get('db.url')


def test_covid_cz_project():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/covid_cz/dbd.project')
    assert project.model_directory_from_project() == os.path.normpath('./tests/fixtures/covid_cz/model')
    assert str(project.alchemy_engine_from_project().url) == profile.db_connections().get('covid_cz').get('db.url')
