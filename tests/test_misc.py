import os


def test_path_join():
    assert os.path.join('.', '/usr/bin') == '/usr/bin'
