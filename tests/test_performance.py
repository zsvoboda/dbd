import glob
import os
import shutil

from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.executors.model_executor import ModelExecutor
from dbd.utils.io_utils import download_file
from dbd.utils.profiling_utils import profile_method


def download_all_ref_files(project):
    model_dir = project.model_directory_from_project()
    new_project_dir = './tmp/covid_czech'
    new_model_dir = f'{new_project_dir}/model'
    if not os.path.exists(new_project_dir):
        os.mkdir(new_project_dir)
        os.mkdir(f'{new_project_dir}/model')
        shutil.copy(f'{model_dir}/../dbd.project', f'{new_project_dir}/dbd.project')
        all_yaml_files = f'{model_dir}/*.yaml'
        all_sql_files = f'{model_dir}/*.sql'
        all_csv_files = f'{model_dir}/*.csv'
        for file in glob.glob(all_yaml_files) + glob.glob(all_sql_files) + glob.glob(all_csv_files):
            shutil.copy(file, new_model_dir)
    for file in os.listdir(model_dir):
        if file.endswith(".ref"):
            csv_file = f'{new_model_dir}/{os.path.splitext(file)[0]}.csv'
            if not os.path.exists(csv_file):
                with open(f'{model_dir}/{file}', 'r') as f:
                    for line in f:
                        # assuming just one URL per file
                        # always rewrites the file with the last URL in the ref file
                        download_file(line.strip(), csv_file)


def test_all():
    all_counts = {}
    all_profile_files = f'./tests/fixtures/performance/dbd.profile.*'
    for profile_file in glob.glob(all_profile_files):
        profile = DbdProfile.load(profile_file)
        project = DbdProject.load(profile, 'tests/fixtures/performance/covid_czech/dbd.project')
        download_all_ref_files(project)
        new_project = DbdProject.load(profile, './tmp/covid_czech/dbd.project')
        model = ModelExecutor(new_project)
        engine = project.alchemy_engine_from_project()
        print(f"Executing performance test for '{engine.dialect.name}'")
        profile_method(f"Test for '{engine.dialect.name}'", lambda: model.execute(engine))

        with engine.connect() as conn:
            counts = {}
            for table in ['city', 'country', 'county', 'covid_event', 'covid_hospitalisation', 'covid_testing',
                          'demography', 'district', 'ext_demografie_2021', 'ext_hospitalizace',
                          'ext_kraj_okres_nakazeni_vyleceni_umrti', 'ext_nakazeni_vyleceni_umrti_testy', 'ext_orp',
                          'ext_osoby', 'ext_souradnice', 'ext_umrti', 'ext_vyleceni', 'ext_zeme']:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = result.fetchall()[0][0]
            all_counts[engine.dialect.name] = counts
            print(f"{engine.dialect.name}: {counts}")
    print(all_counts)
    # print("Executing performance test disabled for now.")
