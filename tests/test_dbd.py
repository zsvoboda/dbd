import importlib


def test_version():
    assert importlib.metadata.version('dbd') == '0.6.3'
