from dbd.config.dbd_profile import DbdProfile


def test_profile_connections():
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    connections = profile.db_connections()
    assert 'basic' in connections
    assert 'data_formats' in connections


def test_profile_connection_engines():
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    basic_engine = profile.alchemy_engine_from_profile('basic')
    assert str(basic_engine.url) == profile.db_connections().get('basic').get('db.url')
    data_formats_engine = profile.alchemy_engine_from_profile('data_formats')
    assert str(data_formats_engine.url) == profile.db_connections().get('data_formats').get('db.url')
