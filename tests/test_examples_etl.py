from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.executors.model_executor import ModelExecutor


def test_bigquery():
    profile = DbdProfile.load('./tests/fixtures/examples/bigquery/etl/dbd.profile')
    project = DbdProject.load(profile, './tests/fixtures/examples/bigquery/etl/dbd.project')
    model = ModelExecutor(project)
    engine = project.alchemy_engine_from_project()
    model.execute(engine)

