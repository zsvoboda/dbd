from typing import Any, List, TypeVar, Tuple, Dict

import sqlalchemy

from dbd.log.dbd_exception import DbdException
from dbd.utils.text_utils import fully_qualified_table_name

TaskType = TypeVar('TaskType', bound='Task')


class InvalidTaskDefinition(DbdException):
    pass


class Task:
    """
    Generic task. Ancestor of all tasks that can be planned.
    """

    TOP_LEVEL_SCHEMA_NAME = '*'
    TASK_ID_DELIMITER = '.'
    TASK_TARGET_PROLOG = 'prolog'
    TASK_TARGET_EPILOG = 'epilog'

    @classmethod
    def generate_task_id(cls, target: str, schema: str):
        """
        Generate task ID
        :param str schema: target schema
        :param str target: target (usually a table)
        :return: task ID
        :rtype: str
        """
        return f"{schema if schema is not None else Task.TOP_LEVEL_SCHEMA_NAME}{Task.TASK_ID_DELIMITER}{target}"

    def __init__(self, task_id: str, target: str, target_schema: str):
        """
        Constructor
        :param str task_id: Task ID
        :param str target: Task target (usually a db table)
        :param str target_schema: Task target db schema
        """
        self.__task_id = task_id
        self.__target = target
        self.__target_schema = target_schema
        self.__data = None

    def task_id(self) -> str:
        """
        Returns task ID
        :return: task ID
        :rtype: str
        """
        return self.__task_id

    def target_schema(self) -> str:
        """
        Returns task target schema
        :return: task target schema
        :rtype: str
        """
        return self.__target_schema

    def target(self) -> str:
        """
        Returns task target (usually a db table) name
        :return: task target (usually a db table) name
        :rtype: str
        """
        return self.__target

    def fully_qualified_target(self) -> str:
        """
        Returns fully qualified task target (usually a db table) name
        :return: fully qualified task target (usually a db table) name
        :rtype: str
        """
        return fully_qualified_table_name(self.__target_schema, self.__target)

    def set_task_data(self, data: Any):
        """
        Sets task specific data (usually a SQL statement)
        :param Any data: task specific data (usually a SQL statement)
        """
        self.__data = data

    def task_data(self) -> Any:
        """
        :return: task specific data (usually a SQL statement)
        :rtype: Any (usually str)
        """
        return self.__data

    def depends_on(self) -> List[str]:
        """
        Returns list of dependencies (list of fully qualified names)
        :return: list of dependencies
        :rtype: List[str]
        """
        pass

    def __eq__(self, other: TaskType) -> bool:
        """
        Comparator
        :param Task other: another Task instance to compare this instance to
        :return: True if both Tasks are equal (have the same task_id), False otherwise
        :rtype: bool
        """
        if isinstance(other, Task):
            if self.task_id() == other.task_id():
                return True
        return False

    def validate(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates task
        :return: True if schema is valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        return True, {}

    def create(self, target_alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine,
               **kwargs) -> None:
        """
        Executes / creates the task
        :param sqlalchemy.MetaData target_alchemy_metadata: MetaData SQLAlchemy MetaData
        :param sqlalchemy.engine.Engine alchemy_engine:
        :param Dict[str, str] copy_stage_storage: copy stage storage parameters e.g. AWS S3 dict(url, access_key, secret_key)
        """
        pass

    def drop(self, alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Drops the target table in the database
        :param sqlalchemy.MetaData alchemy_metadata: SqlAlchemy metadata
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine database connection
        """
        pass
