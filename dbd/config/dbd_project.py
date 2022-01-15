import os
from os.path import exists
from typing import Dict, TypeVar, Any

import sqlalchemy.engine
import yaml

from dbd.config.dbd_profile import DbdProfile
from dbd.log.dbd_exception import DbdException
from dbd.utils.jinja_utils import apply_template

ENV_VARS = {key: str(value) for key, value in os.environ.items()}

DbdProjectType = TypeVar('DbdProjectType', bound='DbdProject')


class DbdProjectConfigException(DbdException):
    pass


class DbdProject:
    """
    Project contains all project-level parameters and options
    """

    def __init__(self, profile: DbdProfile, project_file_full_path: str, config: Dict[str, Any]):
        """
        Constructor
        :param DbdProfile profile: DBD Profile
        :param str project_file_full_path: Path to the project file
        :param Dict[str, Any] config: Config
        """

        if profile is None or not isinstance(profile, DbdProfile):
            raise DbdProjectConfigException("Invalid profile.")
        self.__profile = profile
        self.__project_file = project_file_full_path
        self.__project_directory = os.path.dirname(project_file_full_path)
        self.__config = config

    @classmethod
    def load(cls, profile: DbdProfile, project_file_name='dbd.project') -> DbdProjectType:
        """
        Loads profile config file
        :param DbdProfile profile: DBD Profile
        :param str project_file_name: Profile file name
        :return: new DbdProject
        :rtype: DbdProject
        """
        project_file_name_path = os.path.join('.', project_file_name)
        if exists(project_file_name_path):
            processed_yaml = apply_template(project_file_name_path, ENV_VARS)
            return DbdProject(profile, os.path.normpath(project_file_name_path), yaml.safe_load(processed_yaml))
        raise DbdProjectConfigException(
            f"Can't find DBD project file. Searched the current dir and your home dir for '{project_file_name_path}'")

    def alchemy_engine_from_project(self) -> sqlalchemy.engine.Engine:
        """
        Returns SQLAlchemy engine initialized from project parameters
        :return: SQLAlchemy engine initialized from project parameters
        :rtype: sqlalchemy.engine.Engine
        """
        if self.__config is not None and 'database' in self.__config:
            return self.__profile.alchemy_engine_from_profile(self.__config.get('database'))
        else:
            raise DbdProjectConfigException(f"Project file '{self.__project_directory}{os.sep}{self.__project_file}' "
                                            f"doesn't contain 'database' key.")

    def model_directory_from_project(self) -> str:
        """
        Returns model directory initialized from project
        :return: model directory initialized from project
        :rtype: sqlalchemy.engine.Engine
        """
        model_dir = os.path.join(self.__project_directory, self.__config.get(
            'model')) if self.__config is not None and 'model' in self.__config else './model'
        if exists(model_dir):
            return os.path.normpath(model_dir)
        else:
            raise DbdProjectConfigException(f"Can't find project file '{self.__project_file}' or it doesn't "
                                            f"contain 'model' key and no 'model' dir exists in your current dir.")

    def copy_stage_from_project(self) -> Dict[str, str]:
        """
        Returns copy_stage storage initialized from project
        :return: copy stage (e.g. AWS S3) urls and credentials dict(url, access_key, secret_key)
        :rtype: Dict[str, str]
        """
        if 'copy_stage' in self.__config:
            copy_stage_name = self.__config.get('copy_stage')
            storages = self.__profile.storages()
            if copy_stage_name in storages:
                return storages.get(copy_stage_name)
            else:
                raise DbdProjectConfigException(f"Can't find storage '{copy_stage_name}' "
                                                f"referenced from your project file  '{self.__project_file}' "
                                                f"in your profile file '{self.__profile.profile_path()}'.")
        return None

    def profile(self) -> DbdProfile:
        """
        Returns associated profile
        :return: associated profile
        :rtype: DbdProfile
        """
        return self.__profile
