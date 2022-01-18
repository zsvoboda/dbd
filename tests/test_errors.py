from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.db.db_table import DbTableCreationException
from dbd.executors.model_executor import ModelExecutor
from dbd.tasks.data_task import DbdInvalidDataFileFormatException, DbdDataLoadError, \
    DbdInvalidDataFileReferenceException
from dbd.utils.sql_parser import SQlParserException


def test_validate_invalid():
    profile = DbdProfile.load('./tests/fixtures/capabilities/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_model/dbd.project')
    model = ModelExecutor(project)
    validation_result, validation_errors = model.validate()
    assert not validation_result


def test_validate_invalid_sql():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_sql/dbd.project')
    model = ModelExecutor(project)
    try:
        validation_result, validation_errors = model.validate()
    except SQlParserException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_sql():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_sql/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except SQlParserException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_json():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_json/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_json2():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_json2/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_json3():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_json3/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_fk():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_fk/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbTableCreationException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_data_fk():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_data_fk/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdDataLoadError as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_csv():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_csv/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_excel():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_excel/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_ref():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_ref/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileReferenceException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_ref2():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_ref2/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileReferenceException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_number_format():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_number_format/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False


def test_execute_invalid_date_format():
    profile = DbdProfile.load('./tests/fixtures/errors/dbd.profile')
    project = DbdProject.load(profile, 'tests/fixtures/errors/invalid_date_format/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    try:
        model.execute(engine)
    except DbdInvalidDataFileFormatException as e:
        print(e)
        assert True
    else:
        assert False
