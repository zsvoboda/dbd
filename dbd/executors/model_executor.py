import logging
import os
import re
from os.path import exists
from typing import List, Dict, Any

import click
import networkx as nx
import sqlalchemy.engine
import yaml
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError

from dbd.config.dbd_project import DbdProject
from dbd.log.dbd_exception import DbdException
from dbd.tasks.data_task import DataTask
from dbd.tasks.db_table_task import DbTableTask
from dbd.tasks.ddl_task import DdlTask
from dbd.tasks.elt_task import EltTask
from dbd.tasks.task import Task
from dbd.utils.io_utils import is_url
from dbd.utils.sql_parser import SqlParser
from dbd.utils.text_utils import relative_path_to_base_dir_no_ext, remove_prefix, relative_path_to_base_dir

log = logging.getLogger(__name__)


class ModelExecutionException(DbdException):
    pass


class InvalidModelException(DbdException):
    pass


# noinspection PyMethodMayBeStatic
class ModelExecutor:
    """
    Represents a top level data model blueprint that includes all elements (schema, tables, ELT operations, tests, etc.)
    The model is represented by multiple code files located in a hierarchy of directories on disk.
    """

    def __init__(self, project: DbdProject):
        """
        Constructor
        :param DbdProject project: DBD project
        """
        self.__project = project
        # ensure that there is trailing os.sep
        self.__model_directory = f"{project.model_directory_from_project()}{os.sep}"
        self.__tasks = {}
        self.__ddl_tasks = {}
        self.__metadata_cache = {}

        self.__jinja_model_env = Environment(loader=FileSystemLoader(self.__model_directory))
        self.__jinja_model_env.globals.update()

    def reflect_metadata_cache(self, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Reflects all cached metadata. This is necessary for detecting deletions
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy engine database connection (both target and source)
        """
        # TODO: this approach can be slow - need to figure out, how to rebuild the cache incrementally
        log.debug(f"Reflecting metadata cache.")
        self.__build_metadata_cache(alchemy_engine)

    def validate(self):
        """
        Validates files stored in a model directory.
        The execution is performed in the database connected
        via the SQLAlchemy engine.
        """
        self.__populate_model_from_directory()
        ordered_tasks = self.__order_tasks_by_dependencies()
        task_errors = {}
        validation_result = True
        for task in reversed(ordered_tasks):
            log.debug(f"Validating task: '{task.task_id()}'.")
            task_validation_result, task_validation_errors = task.validate()
            if not task_validation_result:
                task_errors[task.task_id()] = task_validation_errors
                validation_result = False
            log.debug(f"Task validation finished: '{task.task_id()}'.")
        return validation_result, task_errors

    def execute(self, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Executes files stored in a model directory. The execution is performed in the database connected via
        SQLAlchemy engine.
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy engine database connection (both target and source)
        """
        try:
            self.__populate_model_from_directory()
            self.__build_metadata_cache(alchemy_engine)

            ordered_tasks = self.__order_tasks_by_dependencies()
            self.__drop_tables(ordered_tasks, alchemy_engine)
            self.reflect_metadata_cache(alchemy_engine)
            for task in reversed(ordered_tasks):
                schema = task.target_schema() if task.target_schema() is not None else Task.TOP_LEVEL_SCHEMA_NAME
                click.echo(f"Executing task: '{task.task_id()}'.")
                log.debug(f"Executing task: '{task.task_id()}'.")
                task.create(self.__metadata_cache[schema], alchemy_engine,
                            copy_stage_storage=self.__project.copy_stage_from_project())
                log.debug(f"Task execution finished: '{task.task_id()}'.")
        except OperationalError as o:
            log.error(f"Can't execute model because of: '{o}'.")
            raise ModelExecutionException(f"Can't execute model because of: '{o}'.")

    def __build_metadata_cache(self, alchemy_engine: sqlalchemy.engine.Engine):
        # noinspection GrazieInspection
        """
        Builds cache of SQLAlchemy MetaData objects. There is one MetaData object per schema.
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy engine / database connection (both target and source
        schemas) for database objects creation, ELT tasks, etc.
        """
        # distinct schemas
        schemas = list(set([s.target_schema() for s in self.__tasks.values() if s.target_schema() is not None]))
        log.debug(f"Processing schemas: '{schemas}'.")
        for s in schemas:
            m = MetaData(bind=alchemy_engine, schema=s if s is not None and len(s) > 0 else None)
            m.reflect(views=True)
            self.__metadata_cache[s] = m
            log.debug(f"Metadata cache for schema {s} added.")
        # MetaData for the top-level objects and DDL
        m = MetaData(bind=alchemy_engine, schema=None)
        m.reflect(views=True)
        self.__metadata_cache[Task.TOP_LEVEL_SCHEMA_NAME] = m
        log.debug(f"Metadata cache for top level schema added.")

    def __apply_template(self, file: str, params: Dict[Any, Any]) -> str:
        """
        Applies Jinja2 template
        :param str file: Jinja2 template file
        :param Dict[Any, Any] params: parameters
        :return: processed content
        :rtype: str
        """
        template = self.__jinja_model_env.get_template(file)
        return template.render(params)

    # noinspection PyUnusedLocal
    def __read_references(self, processed_file: str, model_root: str) -> List[str]:
        """
        Reads references from processed file
        :param str processed_file: processed file
        :param str model_root: model root directory
        :return: list of references URLs or file names
        :rtype: List[str]
        """
        references = []
        for line in [ln.strip() for ln in processed_file.splitlines()]:
            if len(line) > 0:
                if is_url(line):
                    references.append(line)
                else:
                    references.append(relative_path_to_base_dir_no_ext('', self.__model_directory, line))
        return references

    def __process_reference_file(self, model_root: str, dir_name: str, file_name: str, file_extension: str):
        """
        Process file with multiple references to other files (either on a filesystem or URLs).
        Creates multiple data tasks.
        :param str model_root: model root directory
        :param str dir_name: schema directory
        :param str file_name: data file
        :param str file_extension: data file extension
        """
        file_name_absolute = relative_path_to_base_dir(self.__model_directory, model_root, file_name, file_extension)
        log.debug(f"Processing reference file '{file_name_absolute}'.")
        template_params = dict(schema=dir_name, table=file_name, session=self)
        processed_file = self.__apply_template(file_name_absolute, template_params)
        refs = self.__read_references(processed_file, model_root)
        # do we have corresponding yaml config file?
        task_def = self.__load_yaml_metadata(model_root, dir_name, file_name, template_params)
        task = DataTask.from_code(task_def)
        task.set_data_files(refs)
        self.__tasks[task.task_id()] = task
        log.debug(f"Added reference file task: task_id='{task.task_id()}', task_data='{task.task_data()}'.")

    def __process_data_file(self, model_root: str, dir_name: str, file_name: str, file_extension: str):
        """
        Process CSV file. Creates EltTask
        :param str model_root: model root directory
        :param str dir_name: schema directory
        :param str file_name: data file
        :param str file_extension: data file extension
        """
        file_name_absolute = relative_path_to_base_dir(self.__model_directory, model_root, file_name, file_extension)
        log.debug(f"Processing CSV file '{file_name_absolute}'.")
        template_params = dict(schema=dir_name, table=file_name, session=self)
        # do we have corresponding yaml config file?
        task_def = self.__load_yaml_metadata(model_root, dir_name, file_name, template_params)
        task = DataTask.from_code(task_def)
        task.set_data_files([os.path.join(self.__model_directory, file_name_absolute)])
        self.__tasks[task.task_id()] = task
        log.debug(f"Added data file task: task_id='{task.task_id()}', task_data='{task.task_data()}'.")

    def __process_sql_file(self, model_root: str, dir_name: str, file_name: str, file_extension: str):
        """
        Process SQL (usually INSERT from SELECT) file. Creates EltTask
        :param str model_root: model root directory
        :param str dir_name: schema directory
        :param str file_name: SQL file
        :param str file_extension: SDL file extension
        """

        file_name_absolute = relative_path_to_base_dir(self.__model_directory, model_root, file_name, file_extension)
        log.debug(f"Processing SQL file '{file_name_absolute}'.")
        template_params = dict(schema=dir_name, table=file_name, session=self)
        processed_file = self.__apply_template(file_name_absolute, template_params)
        sql = SqlParser.compact_sql(processed_file)
        # do we have corresponding yaml config file?
        task_def = self.__load_yaml_metadata(model_root, dir_name, file_name, template_params)
        task = EltTask.from_code(task_def)
        task.set_task_data(sql)
        self.__tasks[task.task_id()] = task
        log.debug(f"Added SQL file task: task_id='{task.task_id()}', task_data='{task.task_data()}'.")

    def __process_ddl_file(self, model_root: str, dir_name: str, file_name: str, file_extension: str):
        """
        Process DDL (SQL DDL & DML statements)  file. Creates DdlTask
        :param str model_root: model root directory
        :param str dir_name: schema directory
        :param str file_name: DDL file
        :param str file_extension: DDL file extension
        """
        file_name_absolute = relative_path_to_base_dir(self.__model_directory, model_root, file_name, file_extension)
        log.debug(f"Processing DDL file '{file_name_absolute}'.")
        template_params = dict(schema=dir_name, table=file_name, session=self)
        processed_file = self.__apply_template(file_name_absolute, template_params)
        sql_without_comments = SqlParser.remove_sql_comments(processed_file)
        ddl = re.split(r';\s*$', sql_without_comments, flags=re.MULTILINE)

        task = DdlTask.from_code(file_name, dir_name)
        task.set_task_data(ddl)
        self.__ddl_tasks[task.task_id()] = task
        log.debug(f"Added DDL file task: task_id='{task.task_id()}', task_data='{task.task_data()}'.")

    def __load_yaml_metadata(self, model_root: str, dir_name: str, file_name: str, template_params: Dict[Any, Any]):
        """
        Loads corresponding YAML file with metadata, apply Jinja2 and extracts the table definition
        :param str model_root: model root directory
        :param str dir_name: YAML file directory
        :param str file_name: YAML file name
        :param Dict[Any, Any] template_params: Jinja2 context
        :return Dict[str, Any]: task definition
        """
        yaml_file_name_absolute = relative_path_to_base_dir(self.__model_directory, model_root, file_name, 'yaml')
        task_def = dict(runtime=dict(table=file_name, schema=dir_name))
        if exists(os.path.join(self.__model_directory, yaml_file_name_absolute)):
            log.debug(f"YAML config found '{os.path.join(self.__model_directory, yaml_file_name_absolute)}'.")
            processed_yaml = self.__apply_template(yaml_file_name_absolute, template_params)
            config = yaml.safe_load(processed_yaml)
            task_def.update(config)
            log.debug(f"Task definition enriched to task_def='{task_def}'.")
        return task_def

    def __populate_model_from_directory(self):
        """
        Crawls the model directory and collects all code files to a directory
        hashed by a target's name (e.g. database table name)
        """
        for model_root, dirs, files in os.walk(self.__model_directory, topdown=True):
            for filename in files:
                dir_name = f"{remove_prefix(model_root, self.__model_directory)}".split('/')[-1]
                schema_name = dir_name if len(dir_name) > 0 else None
                file_name, extension = os.path.splitext(filename)
                if extension.lower() == '.sql':
                    self.__process_sql_file(model_root, schema_name, file_name, extension)
                if extension.lower() == '.ddl':
                    self.__process_ddl_file(model_root, schema_name, file_name, extension)
                elif extension.lower() in ['.csv', '.json', '.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt',
                                           '.parquet']:
                    self.__process_data_file(model_root, schema_name, file_name, extension)
                elif extension.lower() in ['.ref']:
                    self.__process_reference_file(model_root, schema_name, file_name, extension)
                elif extension.lower() == '.yaml':
                    pass

    def __drop_tables(self, tasks_ordered_by_dependencies: List[Task], alchemy_engine: sqlalchemy.engine.Engine):
        # noinspection GrazieInspection
        """
                Drops db tables as array of table names. The array should be ordered to satisfy the dependencies.
                :param List[str] tasks_ordered_by_dependencies: array of table names ordered in a way that
                    they can be dropped sequentially
                :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine database connection
                (both target and source schemas) for database objects creation, ELT tasks, etc.
                """
        for task in tasks_ordered_by_dependencies:
            schema = task.target_schema() if task.target_schema() is not None else Task.TOP_LEVEL_SCHEMA_NAME
            if isinstance(task, DbTableTask):
                # TODO: decide whether the default=drop is a good idea
                mode = task.process_def().get('mode', 'drop')
                if mode == 'drop':
                    log.debug(f"Dropping task with task_id='{task.task_id()}'.")
                    click.echo(f"Dropping tables for task_id='{task.task_id()}'.")
                    task.drop(self.__metadata_cache[schema], alchemy_engine)
                    log.debug(f"Dropped task with task_id='{task.task_id()}'.")
                elif mode == 'truncate':
                    log.debug(f"Truncating task with task_id='{task.task_id()}'.")
                    click.echo(f"Truncating tables for task_id='{task.task_id()}'.")
                    task.truncate(self.__metadata_cache[schema], alchemy_engine)
                    log.debug(f"Truncated task with task_id='{task.task_id()}'.")

    def __find_task_by_fully_qualified_target_name(self, fully_qualified_target_name):
        """
        Finds a task with target matching a fully qualified name
        :param fully_qualified_target_name: fully qualified target name
        :return: task with the target with matching name
        """
        found = [t for t in self.__tasks.values() if t.fully_qualified_target() == fully_qualified_target_name]
        if len(found) < 1:
            raise InvalidModelException(
                f"Invalid model: task with fully qualified name '{fully_qualified_target_name}' "
                f"not found!")
        elif len(found) > 1:
            raise InvalidModelException(
                f"Invalid model: multiple tasks with fully qualified name '{fully_qualified_target_name}' "
                f"found!")
        return found[0]

    def __order_tasks_by_dependencies(self) -> List[Task]:
        """
        Order tables by dependencies, so they can be created or dropped sequentially

        :return: list of tasks ordered in a way that satisfies the dependencies
        :rtype: List[Task]
        """
        log.debug(f"Ordering tasks '{self.__tasks.keys()}'.")
        dag_edges = []
        # order task by dependencies (items with most dependencies are at the beginning)
        for task in self.__tasks.values():
            task_table_dependencies = task.depends_on()
            for table_dependency in task_table_dependencies:
                try:
                    task_dependency = self.__find_task_by_fully_qualified_target_name(table_dependency)
                    dag_edges.append((task.task_id(), task_dependency.task_id()))
                except InvalidModelException:
                    # Some dependencies aren't resolved. We expect that these already exist
                    pass
        graph = nx.DiGraph()
        graph.add_edges_from(dag_edges)
        ordered_task_ids = list(nx.topological_sort(graph))
        ordered_tasks = [self.__tasks[tid] for tid in ordered_task_ids]
        independent_tasks = []
        for task in self.__tasks.values():
            if task not in ordered_tasks:
                independent_tasks.append(task)
        dag_order = independent_tasks + ordered_tasks
        # now we need to place the DDL tasks prolog at the end of each schema, epilog at the beginning
        # first remove the global epilog and prolog if they exist
        global_prolog = self.__ddl_tasks.pop(
            f"{Task.TOP_LEVEL_SCHEMA_NAME}{Task.TASK_ID_DELIMITER}{Task.TASK_TARGET_PROLOG}", None)
        global_epilog = self.__ddl_tasks.pop(
            f"{Task.TOP_LEVEL_SCHEMA_NAME}{Task.TASK_ID_DELIMITER}{Task.TASK_TARGET_EPILOG}", None)
        # Now global epilog and prolog are gone, we are with schema epilogs and prologs
        for epilog in [x for x in self.__ddl_tasks.values() if Task.TASK_TARGET_EPILOG == x.target()]:
            epilog_schema = epilog.target_schema()
            first_schema_index = self.find_first_task_for_schema(dag_order, epilog_schema)
            if first_schema_index >= 0:
                dag_order.insert(first_schema_index, epilog)
            else:
                dag_order.insert(0, epilog)

        for prolog in [x for x in self.__ddl_tasks.values() if Task.TASK_TARGET_PROLOG == x.target()]:
            prolog_schema = prolog.target_schema()
            last_schema_index = self.find_last_task_for_schema(dag_order, prolog_schema)
            if last_schema_index >= 0:
                dag_order.insert(last_schema_index + 1, prolog)
            else:
                dag_order.insert(len(dag_order), prolog)

        # global epilogs and prologs to the very beginning / end
        if global_prolog:
            dag_order.append(global_prolog)
        if global_epilog:
            dag_order.insert(0, global_epilog)
        log.debug(f"Task order '{[t.task_id() for t in dag_order]}'.")
        return dag_order

    def find_first_task_for_schema(self, tasks: List[Task], schema: str) -> int:
        """
        Finds the first task for schema
        :param List[Task] tasks: list of tasks
        :param str schema: schema name
        :return: the first index of task with the schema
        :rtype: int
        """
        for i in range(len(tasks)):
            if schema == tasks[i].target_schema():
                return i
        return -1

    def find_last_task_for_schema(self, tasks: List[Task], schema: str) -> int:
        """
        Finds the last task for schema
        :param List[Task] tasks: list of tasks
        :param str schema: schema name
        :return: the first index of task with the schema
        :rtype: int
        """
        for i in reversed(range(len(tasks))):
            if schema == tasks[i].target_schema():
                return i
        return -1
