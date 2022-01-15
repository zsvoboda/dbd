import logging
import os
from os.path import exists
from pathlib import Path
from typing import Dict, Any, TypeVar

import sqlalchemy.engine
import yaml
from sqlalchemy import engine_from_config

from dbd.log.dbd_exception import DbdException
from dbd.utils.jinja_utils import apply_template

log = logging.getLogger(__name__)

ENV_VARS = {key: str(value) for key, value in os.environ.items()}

CONFIG_PREFIX = 'db.'

DbdProfileType = TypeVar('DbdProfileType', bound='DbdProfile')


class DbdProfileConfigException(DbdException):
    pass


class DbdProfile:
    """
    Profile contains all generic parameters and options (e.g. database connections)
    """

    def __init__(self, profile_file_full_path: str, config: Dict[str, Any]):
        """
        Constructor
        :param str profile_file_full_path: Full path to the profile file
        :param Dict[str, str] config: Profile config
        """
        self.__profile_file = profile_file_full_path
        self.__config = config

    def profile_path(self):
        """
        Return path to the profile file
        :return: path to the profile file
        :rtype: str
        """
        return os.path.normpath(self.__profile_file)

    @classmethod
    def load(cls, profile_file_name='dbd.profile') -> DbdProfileType:
        """
        Loads profile config file
        :param str profile_file_name: Profile file name
        :return: new DbdProfile
        :rtype: DbdProfile
        """
        for profile_file_name in [os.path.join('.', profile_file_name),
                                  os.path.join(str(Path.home()), 'dbd.profile')]:
            if exists(profile_file_name):
                processed_yaml = apply_template(profile_file_name, ENV_VARS)
                return DbdProfile(os.path.normpath(profile_file_name), yaml.safe_load(processed_yaml))

        raise DbdProfileConfigException(
            f"Can't find DBD profile file. Searched the current dir and your home dir for '{profile_file_name}'")

    def db_connections(self) -> Dict[str, Dict[str, Any]]:
        """
        Return database connections from the profile
        :return: database connections from the profile
        :rtype: Dict[str, Dict[str, Any]]
        """
        if self.__config is not None:
            if 'databases' in self.__config:
                return self.__config.get('databases')
            else:
                raise DbdProfileConfigException(
                    f"Your dbd profile '{self.__profile_file}' doesn't contain 'database' key.")

    def storages(self) -> Dict[str, Dict[str, Any]]:
        """
        Return copy stage from the profile
        :return: copy stage from the profile
        :rtype: Dict[str, Dict[str, Any]]
        """
        if self.__config is not None:
            if 'storages' in self.__config:
                return self.__config.get('storages')
            else:
                raise DbdProfileConfigException(
                    f"Your dbd profile '{self.__profile_file}' doesn't contain 'storages' key.")

    def alchemy_engine_from_profile(self, connection_name: str) -> sqlalchemy.engine.Engine:
        """
        Returns SQLAlchemy engine initialized from profile
        :param str connection_name: connection name
        :return: SQLAlchemy engine initialized from profile
        :rtype: sqlalchemy.engine.Engine
        """
        databases = self.db_connections()
        if connection_name in databases:
            try:
                log.debug(f"Connecting to database '{connection_name}'")
                return engine_from_config(databases.get(connection_name), prefix=CONFIG_PREFIX)
            except Exception as e:
                raise DbdProfileConfigException(f"Invalid connection '{connection_name}' config in the profile file "
                                                f"'{self.__profile_file}'. Underlying error: '{e}'.")
        else:
            raise DbdProfileConfigException(f"Connection '{connection_name}' isn't defined in the profile file "
                                            f"'{self.__profile_file}'.")
