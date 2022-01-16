import csv
import logging
import os
import tempfile
from datetime import datetime, date
from io import StringIO
from typing import Dict, List, Any, TypeVar

import click
import math
import pandas as pd
import s3fs
import sqlalchemy.engine
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import Column, TEXT, TIMESTAMP, DATE, INT, FLOAT, BOOLEAN

from dbd.config.dbd_project import DbdProjectConfigException
from dbd.db.db_table import DbTable
from dbd.log.dbd_exception import DbdException
from dbd.tasks.db_table_task import DbTableTask
from dbd.utils.io_utils import download_file, url_to_filename
from dbd.utils.io_utils import is_url
from dbd.utils.sql_parser import SqlParser

log = logging.getLogger(__name__)


class UnsupportedDataFile(DbdException):
    pass


DataTaskType = TypeVar('DataTaskType', bound='DataTask')


def psql_writer(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


class DataTask(DbTableTask):
    """
    Data loading task. Loads data from a local data file (e.g. CSV) to database.
    """

    def __init__(self, task_def: Dict[str, Any]):
        """
        Data task constructor
        :param Dict[str, Any] task_def: Target table definition
        """
        super().__init__(task_def)

    def data_files(self) -> List[str]:
        """
        Task data files
        :return: task data files

        """
        return self.task_data()

    def set_data_files(self, data_files: List[str]):
        """
        Sets task data files
        :param List[str] data_files: task data file
        """
        self.set_task_data(data_files)

    @classmethod
    def from_code(cls, task_def: Dict[str, Any]) -> DataTaskType:
        """
        Creates a new task from table definition (dict)
        :param Dict[str, Any] task_def: table definition (dict)
        :return: new EltTask instance
        :rtype: EltTask
        """

        return DataTask(task_def)

    # noinspection PyMethodMayBeStatic
    def __data_file_columns(self, data_frame: pd.DataFrame) -> List[sqlalchemy.Column]:
        """
        Introspects data file columns
        :param pd.DataFrame data_frame: Pandas dataframe with populated data
        :return: list of data file columns (SQLAlchemy Column[])
        :rtype: List[sqlalchemy.Column]
        """
        columns = []
        for column_name, column_type in data_frame.dtypes.iteritems():
            if column_type.name == 'datetime64[ns]':
                columns.append(Column(column_name, TIMESTAMP))
            elif column_type.name == 'datetime64[D]':
                columns.append(Column(column_name, DATE))
            elif column_type.name == 'object':
                columns.append(Column(column_name, TEXT))
            elif column_type.name == 'int64':
                columns.append(Column(column_name, INT))
            elif column_type.name == 'float64':
                columns.append(Column(column_name, FLOAT))
            elif column_type.name == 'bool':
                columns.append(Column(column_name, BOOLEAN))
            else:
                columns.append(Column(column_name, TEXT))
        return columns

        # noinspection DuplicatedCode

    def __override_data_file_column_definitions(self, data_frame: pd.DataFrame) -> Dict[str, Any]:
        """
        Merges the data file column definitions with the column definitions from the task_def.
        The column definitions override the introspected data file types
        :param pd.DataFrame data_frame: Pandas dataframe with populated data
        :return: data file columns overridden with the task's explicit column definitions
        :rtype: Dict[str, Any]
        """
        table_def = self.table_def()
        column_overrides = table_def.get('columns', {})
        data_file_columns = self.__data_file_columns(data_frame)
        ordered_columns = {}
        for c in data_file_columns:
            overridden_column = column_overrides.get(c.name)
            if overridden_column:
                if 'type' not in overridden_column:
                    overridden_column['type'] = c.type
                ordered_columns[c.name] = overridden_column
            else:
                ordered_columns[c.name] = {"type": c.type}

        table_def['columns'] = ordered_columns
        return table_def

    def create(self, target_alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine,
               **kwargs) -> None:
        """
        Executes the task. Creates the target table and loads data
        :param sqlalchemy.MetaData target_alchemy_metadata: MetaData SQLAlchemy MetaData
        :param Dict[str, str] copy_stage_storage: copy stage storage parameters e.g. AWS S3 dict(url, access_key, secret_key)
        :param sqlalchemy.engine.Engine alchemy_engine:
        """
        copy_stage_storage = kwargs.get('copy_stage_storage')
        for data_file in self.data_files():
            if len(data_file) > 0:
                with tempfile.TemporaryDirectory() as tmpdirname:
                    if is_url(data_file):
                        absolute_file_name = os.path.join(tmpdirname, url_to_filename(data_file))
                        click.echo(f"\tDownloading file: '{data_file}'.")
                        download_file(data_file, absolute_file_name)
                    else:
                        absolute_file_name = data_file

                    df = self.__read_file_to_dataframe(absolute_file_name)

                    mysql_bulk_load_config = alchemy_engine.url.query.get('local_infile') == '1'

                    if self.db_table() is None:
                        table_def = self.__override_data_file_column_definitions(df)
                        db_table = DbTable.from_code(self.target(), table_def, target_alchemy_metadata,
                                                     self.target_schema())
                        self.set_db_table(db_table)
                        db_table.create()
                    dtype = self.__adjust_dataframe_datatypes(df, alchemy_engine.dialect.name)
                    click.echo(f"\tLoading data to database.")
                    if alchemy_engine.dialect.name == 'snowflake':
                        self.__bulk_load_snowflake(df, alchemy_engine)
                    elif alchemy_engine.dialect.name == 'postgresql':
                        df.to_sql(self.target(), alchemy_engine, chunksize=16384, method=psql_writer,
                                  schema=self.target_schema(), if_exists='append', index=False, dtype=dtype)
                    elif alchemy_engine.dialect.name == 'mysql' and mysql_bulk_load_config:
                        self.__bulk_load_mysql(df, alchemy_engine)
                    elif alchemy_engine.dialect.name == 'bigquery':
                        self.__bulk_load_bigquery(df, dtype, alchemy_engine)
                    elif alchemy_engine.dialect.name == 'redshift' and copy_stage_storage is not None:
                        self.__bulk_load_redshift(df, alchemy_engine, copy_stage_storage)
                    else:
                        if alchemy_engine.dialect.name == 'redshift':
                            log.warning("Using default SQLAlchemy writer for Redshift. Specify 'copy_stage' parameter "
                                        "in your profile configuration file to make loading faster.")
                        if alchemy_engine.dialect.name == 'mysql':
                            log.warning("Using default SQLAlchemy writer for MySQL. Specify 'local_infile=1' parameter "
                                        "in a query parameter of your MySQL connection string to make loading faster.")
                        df.to_sql(self.target(), alchemy_engine, chunksize=16384, method='multi',
                                  schema=self.target_schema(), if_exists='append', index=False, dtype=dtype)

    def __bulk_load_bigquery(self, df: pd.DataFrame, dtype: Dict[str, str], alchemy_engine: sqlalchemy.engine.Engine):
        """
        Bulk load data to BigQuery
        :param pd.DataFrame df: pandas dataframe
        :param Dict[str, str] dtype: Data types for each column
        :param sqlalchemy.engine.Engine alchemy_engine: SqlAlchemy engine
        """
        table_schema = [dict(name=k, type=SqlParser.datatype_to_gbq_datatype(str(v))) for (k, v) in dtype.items()]
        target_schema = self.target_schema()
        dataset = target_schema if target_schema is not None and len(target_schema) > 0 \
            else alchemy_engine.engine.url.database
        df.to_gbq(f"{dataset}.{self.target()}", if_exists='append', table_schema=table_schema)

    def __bulk_load_redshift(self, df: pd.DataFrame, alchemy_engine: sqlalchemy.engine.Engine,
                             copy_stage_storage: Dict[str, str]):
        """
        Bulk load data to Redshift
        :param pd.DataFrame df: pandas dataframe
        :param sqlalchemy.engine.Engine alchemy_engine: SqlAlchemy engine
        :param Dict[str, str] copy_stage_storage: copy stage storage parameters e.g. AWS S3 dict(url, access_key, secret_key)
        """
        if copy_stage_storage is not None:
            if 'url' in copy_stage_storage:
                aws_stage_path = copy_stage_storage['url']
            else:
                raise DbdProjectConfigException(
                    "Missing 'url' key in the 'copy_stage' storage definition parameter in your profile file.")
            if 'access_key' in copy_stage_storage:
                aws_access_key = copy_stage_storage['access_key']
            else:
                raise DbdProjectConfigException(
                    "Missing 'access_key' key in the 'copy_stage' storage definition parameter in your profile file.")
            if 'secret_key' in copy_stage_storage:
                aws_secret_key = copy_stage_storage['secret_key']
            else:
                raise DbdProjectConfigException(
                    "Missing 'secret_key' key in the 'copy_stage' storage definition parameter in your profile file.")

            temp_file_name = f"{aws_stage_path.rstrip('/')}/{self.target_schema()}/{self.target()}" \
                             f"_{datetime.now().strftime('%y%m%d_%H%M%S')}"

            df.to_csv(f"{temp_file_name}.csv.gz", index=False, compression='gzip',
                      storage_options={"key": aws_access_key,
                                       "secret": aws_secret_key})
            with alchemy_engine.connect() as conn:
                conn.execute(f"copy {self.target()} from '{temp_file_name}.csv.gz' "
                             f"CREDENTIALS 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}' "
                             f" DELIMITER AS ',' DATEFORMAT 'YYYY-MM-DD' EMPTYASNULL IGNOREHEADER 1 GZIP")
                conn.connection.commit()
            file = s3fs.S3FileSystem(anon=False, key=aws_access_key, secret=aws_secret_key)
            file.rm(f"{temp_file_name}.csv.gz")
        else:
            raise DbdProjectConfigException("Redshift requires 'copy_stage' parameter in your project file.")

    def __bulk_load_snowflake(self, df: pd.DataFrame, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Bulk load data to snowflake
        :param pandas.DataFrame df: DataFrame
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy engine
        """
        df.columns = map(str.upper, df.columns)
        table_name = self.target().upper()
        schema_name = self.target_schema()
        schema_name = schema_name.upper() if schema_name else None
        with alchemy_engine.connect() as conn:
            write_pandas(
                conn.connection, df,
                table_name=table_name,
                schema=schema_name,
                quote_identifiers=False)
            conn.connection.commit()

    def __bulk_load_mysql(self, df: pd.DataFrame, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Bulk load data to MySQL
        :param pandas.DataFrame df: DataFrame
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy engine
        """
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            temporary_file_name = f"{tmp_dir_name}/bulk.csv"
            df.to_csv(temporary_file_name, index=False, na_rep='\\N')
            target_schema = self.target_schema()
            target_schema_with_dot = f"{target_schema}." if target_schema else ''
            with alchemy_engine.connect() as conn:
                query = f"LOAD DATA LOCAL INFILE '{temporary_file_name}' " \
                        f"INTO TABLE {target_schema_with_dot}{self.target()} " \
                        f"FIELDS TERMINATED BY ',' " \
                        f"OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' IGNORE 1 LINES"
                conn.execute(query)
                conn.connection.commit()

    def __adjust_dataframe_datatypes(self, df, dialect_name: str):
        """
        Adjusts the dataframe datatypes to match the target table
        :param pd.DataFrame df: Pandas dataframe with populated data
        :param str dialect_name: SQLAlchemy dialect name
        :return: dtype for to_sql
        """
        dtype = {}
        for c in self.db_table().columns():
            column_name = c.name()
            column_type = c.type()
            python_type = SqlParser.parse_alchemy_data_type(column_type).python_type
            if isinstance(python_type, type) and issubclass(python_type, datetime):
                if dialect_name in ['bigquery']:
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                    df[column_name] = df[column_name].astype('datetime64[ns]')
                elif dialect_name in ['snowflake']:
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
            elif isinstance(python_type, type) and issubclass(python_type, date):
                if dialect_name in ['bigquery']:
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.strftime('%Y-%m-%d')
                    df[column_name] = df[column_name].astype('datetime64[ns]')
                elif dialect_name in ['snowflake']:
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.strftime('%Y-%m-%d')
                else:
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
            elif isinstance(python_type, type) and issubclass(python_type, bool):
                if dialect_name in ['mysql']:
                    df[column_name] = df[column_name].map(lambda x: SqlParser.parse_bool_int(x))
                    df[column_name] = df[column_name].astype('float').astype('Int64')
                else:
                    df[column_name] = df[column_name].map(lambda x: SqlParser.parse_bool(x))
                    df[column_name] = df[column_name].astype('boolean')
            elif isinstance(python_type, type) and issubclass(python_type, int):
                df[column_name] = df[column_name].astype('float').astype('Int64')
            elif isinstance(python_type, type) and issubclass(python_type, float):
                df[column_name] = df[column_name].astype(python_type)
            else:
                # consistently interpret "" as NULL
                df[column_name] = df[column_name].map(lambda x: SqlParser.parse_string(x))
                df[column_name] = df[column_name].astype('object')
            dtype[column_name] = column_type
        return dtype

    # noinspection PyMethodMayBeStatic
    def __read_file_to_dataframe(self, absolute_file_name: str) -> pd.DataFrame:
        """
        Read the file content to Pandas dataframe
        :param str absolute_file_name: filename to read the dataframe from
        :return: Pandas DataFrame
        :rtype: pd.DataFrame
        """
        if is_url(absolute_file_name):
            absolute_file_name = download_file(absolute_file_name, absolute_file_name)
        file_name, file_extension = os.path.splitext(absolute_file_name)
        if file_extension.lower() == '.csv':
            return pd.read_csv(absolute_file_name, dtype=str)
        elif file_extension.lower() == '.json':
            # noinspection PyTypeChecker
            return pd.read_json(absolute_file_name)
        elif file_extension.lower() in {'.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt'}:
            return pd.read_excel(absolute_file_name)
        elif file_extension.lower() == '.parquet':
            return pd.read_parquet(absolute_file_name, use_nullable_dtypes=True)
        else:
            raise UnsupportedDataFile(f"Data files with extension '{file_extension}' aren't supported.")
