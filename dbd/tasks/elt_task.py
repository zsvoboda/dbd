import hashlib
from typing import Dict, List, Any, TypeVar

import click
import sqlalchemy.engine
from sqlalchemy import select, Column
from sqlalchemy.testing.schema import Table

from dbd.db.db_table import DbTable
from dbd.tasks.db_table_task import DbTableTask
from dbd.utils.sql_parser import SqlParser
from dbd.utils.text_utils import fully_qualified_table_name

REFLECTION_TMP_VIEW_PREFIX = "tmp_view_"

EltTaskType = TypeVar('EltTaskType', bound='EltTask')


# noinspection PyMethodMayBeStatic
class EltTask(DbTableTask):
    """
    Database ELT task (INSERT from SELECT) definition
    """

    def __init__(self, task_def: Dict[str, Any]):
        """
        EltTask constructor
        :param Dict[str, Any] task_def: Target table definition
        """
        super().__init__(task_def)

    def __create_tmp_reflection_view(self, fully_qualified_view_name: str,
                                     alchemy_engine: sqlalchemy.engine.Engine):
        """
        Creates a temporary helper database view for introspecting the ELT task's
        SELECT result's structure (columns and types)
        TODO: some more clever way of introspection without creating a database view
        :param str fully_qualified_view_name: str fully qualified view name
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy Engine
        """
        view_sql_create = f"CREATE VIEW {fully_qualified_view_name} AS {self.sql()}"
        with alchemy_engine.connect() as conn:
            conn.execute(view_sql_create)

    def __drop_tmp_reflection_view(self, fully_qualified_view_name: str, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Drops the temporary helper database view for introspecting the ELT task's
        SELECT result's structure (columns and types)
        TODO: some more clever way of introspection without creating a database view
        :param str fully_qualified_view_name: fully qualified view name
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy Engine
        """
        view_sql_drop = f"DROP VIEW IF EXISTS {fully_qualified_view_name}"
        with alchemy_engine.connect() as conn:
            conn.execute(view_sql_drop)

    def __sql_columns(self, target_alchemy_metadata: sqlalchemy.MetaData,
                      alchemy_engine: sqlalchemy.engine.Engine) -> List[sqlalchemy.Column]:
        """
        Introspects ETL task SELECT statement structure (result's column names and types)
        TODO: some more clever way of introspection without creating a database view
        :param sqlalchemy.MetaData target_alchemy_metadata: SQLAlchemy MetaData
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy Engine
        :return: list of the SELECT result columns (SQLAlchemy Column[])
        :rtype: List[sqlalchemy.Column]
        """
        target_db_schema = self.target_schema()

        tmp_name = hashlib.md5(self.sql().encode('utf-8')).hexdigest()
        view_name = f"{REFLECTION_TMP_VIEW_PREFIX}{tmp_name}"
        fully_qualified_view_name = fully_qualified_table_name(target_db_schema, view_name)
        db_con = alchemy_engine.connect()
        self.__drop_tmp_reflection_view(fully_qualified_view_name, db_con)
        self.__create_tmp_reflection_view(fully_qualified_view_name, db_con)
        # SQLAlchemy reflection / autoload doesn't work with fully qualified view name
        tmp_reflection_view = Table(view_name, target_alchemy_metadata, autoload=True)
        self.__drop_tmp_reflection_view(fully_qualified_view_name, db_con)
        db_con.close()
        temp_view_select_all_columns = select(tmp_reflection_view.columns).select_from(tmp_reflection_view)
        return [Column(c.name, c.type) for c in temp_view_select_all_columns.selected_columns]

    # noinspection DuplicatedCode
    def __override_sql_column_definitions(self, target_alchemy_metadata: sqlalchemy.MetaData,
                                          alchemy_engine: sqlalchemy.engine.Engine) -> Dict[str, Any]:
        """
        Merges the SQL result column definitions with the column definitions from the ELT task.
        The column definitions override the introspected SQL types
        :param sqlalchemy.MetaData target_alchemy_metadata: MetaData SQLAlchemy MetaData
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy Engine
        :return: ETL task definition merged from the SELECT result structure overridden
            with the ELT task's explicit column definitions
        :rtype: Dict[str, Any]
        """
        table_def = self.table_def()
        column_overrides = table_def.get('columns', {})
        tmp_reflection_view_columns = self.__sql_columns(target_alchemy_metadata, alchemy_engine)
        ordered_columns = {}
        for c in tmp_reflection_view_columns:
            overridden_column = column_overrides.get(c.name)
            if overridden_column:
                if 'type' not in overridden_column:
                    overridden_column['type'] = c.type
                ordered_columns[c.name] = overridden_column
            else:
                ordered_columns[c.name] = {"type": c.type}

        table_def['columns'] = ordered_columns
        return table_def

    def sql(self) -> str:
        """
        Task SQL statement
        :return: task SQL statement
        """
        return self.task_data()

    def set_sql(self, sql: str):
        """
        Sets task SQL
        :param sql: SQL statement
        """
        self.set_task_data(sql)

    @classmethod
    def from_code(cls, task_def: Dict[str, Any]) -> EltTaskType:
        """
        Creates a new ELT task from table definition (dict)
        :param Dict[str, Any] task_def: ELT target table definition
        :return: new EltTask instance
        :rtype: EltTask
        """

        return EltTask(task_def)

    def create(self, target_alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine, **kwargs):
        """
        Executes / creates the task
        :param sqlalchemy.MetaData target_alchemy_metadata: MetaData SQLAlchemy MetaData
        :param sqlalchemy.engine.Engine alchemy_engine:
        """
        process_def = self.process_def()
        materialization = process_def.get('materialization', 'table')
        if materialization == 'view':
            sql_text = f"CREATE VIEW  {self.fully_qualified_target()} AS {self.sql()}"
            with alchemy_engine.connect() as conn:
                click.echo(f"\tCreating view '{self.fully_qualified_target()}'.")
                conn.execute(sql_text)
        else:
            overridden_def = self.__override_sql_column_definitions(target_alchemy_metadata, alchemy_engine)
            db_table = DbTable.from_code(self.target(), overridden_def, target_alchemy_metadata, self.target_schema())
            self.set_db_table(db_table)
            click.echo(f"\tCreating table '{self.fully_qualified_target()}'.")
            db_table.create()
            columns = [c.name() for c in db_table.columns()]
            click.echo(f"\tExecuting SQL.")
            sql_text = f"INSERT INTO {self.fully_qualified_target()}({','.join(columns)}) {self.sql()}"
            with alchemy_engine.connect() as conn:

                conn.execute(sql_text)

    def depends_on(self) -> List[str]:
        """
        Returns list of tables that the ELT task depends on
        :return: list of tables that the ELT task depends on
        :rtype: List[str]
        """
        target_db_schema = self.target_schema()
        foreign_key_tables = super(EltTask, self).depends_on()
        sql_tables = SqlParser.extract_tables(self.sql())
        return foreign_key_tables + [
            f"{fully_qualified_table_name(target_db_schema, t)}" if len(t.split('.')) < 2 else t for t in sql_tables]
