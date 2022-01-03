from dbd.config.dbd_profile import DbdProfile


def test_profile_connections():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    connections = profile.db_connections()
    assert 'states' in connections
    assert 'covid_cz' in connections


def test_profile_connection_engines():
    profile = DbdProfile.load('./tests/fixtures/dbd.profile')
    states_engine = profile.alchemy_engine_from_profile('states')
    assert str(states_engine.url) == profile.db_connections().get('states').get('db.url')
    covid_cz_engine = profile.alchemy_engine_from_profile('covid_cz')
    assert str(covid_cz_engine.url) == profile.db_connections().get('covid_cz').get('db.url')
