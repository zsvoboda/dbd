import importlib


def test_version():
    assert importlib.metadata.version('dbd') == '0.5.9'
