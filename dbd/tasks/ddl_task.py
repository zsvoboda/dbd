from typing import TypeVar

import sqlalchemy.engine
from sqlalchemy import text

from dbd.tasks.task import Task

DdlTaskType = TypeVar('DdlTaskType', bound='DdlTask')


class DdlTask(Task):
    """
    Task for executing SQL script
    """

    def __init__(self, target: str, target_schema: str):
        """
        Constructor
        :param str target: target (epilogue or prologue)
        :param str target_schema: Target schema for executing the SQL script
        """
        task_id = Task.generate_task_id(target, target_schema)
        super().__init__(task_id, target, target_schema)

    def sql_text(self) -> str:
        """
        Table definition dict
        :return: SQL script
        :rtype: str
        """
        return self.task_data()

    def set_sql_text(self, sql_script: str):
        """
        Sets SQL script (multiple SQL DDL statements separated by semicolon
        :param str sql_script: SQL script
        """
        self.set_task_data(sql_script)

    def create(self, target_alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine, **kwargs):
        """
        Executes / creates the task
        :param sqlalchemy.MetaData target_alchemy_metadata: MetaData SQLAlchemy MetaData
        :param sqlalchemy.engine.Engine alchemy_engine:
        """
        for statement in self.sql_text():
            if statement:
                with alchemy_engine.connect() as conn:
                    conn.execute(text(statement))

    @classmethod
    def from_code(cls, target: str, target_schema: str) -> DdlTaskType:
        """
        Creates a new DDL task
        :param str target: target (epilogue or prologue)
        :param str target_schema: target schema
        :return: new DdlTask instance
        :rtype: DdlTask
        """

        return DdlTask(target, target_schema)
