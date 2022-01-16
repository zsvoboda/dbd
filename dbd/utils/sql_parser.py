import logging
import re
import sys
# noinspection PyUnresolvedReferences
from datetime import date, datetime
from typing import List

import math
import numpy as np
import pandas as pd
import sqlalchemy
from dateutil import parser as date_parser
from math import nan
from sql_metadata import Parser

from dbd.log.dbd_exception import DbdException

log = logging.getLogger(__name__)


class SQlParserException(DbdException):
    pass


class SqlParser:
    """ Parses SQL and extracts different parts from the parsed SQL statement."""

    @classmethod
    def extract_tables(cls, sql: str) -> List[str]:
        """
        Extracts tables from the parsed SQL statement.
        Works with subquery, CTE, and many other SQL constructs
        :param str sql: parsed SQL statement
        :return: list of tables that the SQL statement depends on
        :rtype: List[str]
        """
        try:
            tables = Parser(sql).tables
        except ValueError:
            raise SQlParserException(f"Invalid SQL query '{sql}'")
        return tables

    @classmethod
    def extract_foreign_key_tables(cls, foreign_keys_def: List[str]) -> List[str]:
        """
        Extracts tables that the passed foreign keys depend on
        Relies on the <table>.<column> format
        :param List[str] foreign_keys_def: foreign key
        :return: str array of table names extracted from passed foreign keys
        :rtype: List[str]
        """
        for f in foreign_keys_def:
            if len(f.split('.')) <= 1:
                raise SQlParserException(f"Invalid foreign key format '{f}'. There is no table component.")
        return [f.split('.')[0] for f in foreign_keys_def]

    @classmethod
    def compact_sql(cls, sql: str) -> str:
        """
        Compacts the SQL text. Strip comments, etc.
        :param sql: input SQL
        :return: compacted SQL text
        :rtype: str
        """
        try:
            parsed_sql = Parser(sql).without_comments
        except ValueError:
            raise SQlParserException(f"Invalid SQL query '{sql}'")
        return parsed_sql

    @classmethod
    def comments(cls, sql: str) -> List[str]:
        """
        Returns the SQL comments
        :param sql: input SQL
        :return: comments as array of string
        :rtype: List[str]
        """
        try:
            parsed_sql = Parser(sql).comments
        except ValueError:
            raise SQlParserException(f"Invalid SQL query '{sql}'")
        return parsed_sql

    @classmethod
    def parse_alchemy_data_type(cls, data_type: str) -> sqlalchemy.types.TypeEngine:
        """
        Parses SQLAlchemy datatype from string
        :param str data_type: SQLAlchemy data type as string
        :return: SQLAlchemy data type
        :rtype:  sqlalchemy.types.TypeEngine subclass
        """
        parsed_data_type = Parser(f"CREATE TABLE a( c {data_type} )")
        # parsed_data_type = Parser(data_type)
        core_data_type = parsed_data_type.tokens[5].value
        length = int(parsed_data_type.tokens[7].value) if len(
            parsed_data_type.tokens) > 7 and parsed_data_type.tokens[7].is_integer else None
        scale = int(parsed_data_type.tokens[9].value) if len(
            parsed_data_type.tokens) > 9 and parsed_data_type.tokens[9].is_integer else None

        try:
            datatype_class = getattr(sys.modules['sqlalchemy.sql.sqltypes'], core_data_type)
        except AttributeError:
            try:
                datatype_class = getattr(sys.modules['sqlalchemy.dialects.postgresql'], core_data_type)
            except AttributeError:
                try:
                    datatype_class = getattr(sys.modules['sqlalchemy.dialects.bigquery'], core_data_type)
                except AttributeError:
                    log.debug(f"Unsupported data type {core_data_type}.")
                    raise SQlParserException(f"Unsupported data type {core_data_type}.")

        if core_data_type in ('CHAR', 'VARCHAR'):
            return datatype_class(length=length)
        elif core_data_type in ('DECIMAL', 'NUMERIC'):
            return datatype_class(precision=length, scale=scale)
        elif core_data_type in ('TIMESTAMP', 'DATE', 'DATETIME', 'INTEGER', 'FLOAT', 'DOUBLE', 'TEXT', 'SMALLINT',
                                'DOUBLE_PRECISION', 'REAL', 'BOOLEAN', 'BOOL'):
            return datatype_class()
        else:
            log.debug(f"Unsupported data type {core_data_type}.")
            raise SQlParserException(f"Unsupported data type {core_data_type}.")

    @classmethod
    def parse_date(cls, dt: str) -> date:
        """
        Parses a date string
        :param str dt: date string
        :return: parsed date
        :rtype: datetime.date
        """
        dtt = cls.parse_datetime(dt)
        return dtt.date() if isinstance(dtt, datetime) else dtt

    @classmethod
    def parse_datetime(cls, dt: str) -> datetime:
        """
        Parses a datetime string
        :param str dt: datetime string
        :return: parsed datetime
        :rtype: datetime.datetime
        """
        if isinstance(dt, str):
            return date_parser.parse(dt) if len(dt) > 0 else nan
        else:
            return dt

    @classmethod
    def parse_bool(cls, b: str) -> bool:
        """
        Parses a bool string
        :param str b: bool string
        :return: parsed bool
        :rtype: bool
        """
        if isinstance(b, str):
            return b.lower() in ('true', '1', 't', 'y', 'yes')
        elif isinstance(b, bool):
            return b
        elif isinstance(b, int):
            return b != 0
        elif isinstance(b, float):
            return (b != 0.0) if not math.isnan(b) else nan
        else:
            return b

    @classmethod
    def parse_bool_int(cls, b: str) -> int:
        """
        Parses a bool string
        :param str b: bool string
        :return: parsed bool as int (1/0)
        :rtype: int
        """
        if isinstance(b, str):
            return 1 if b.lower() in ('true', '1', 't', 'y', 'yes') else 0
        elif isinstance(b, bool):
            return 1 if b else 0
        elif isinstance(b, int):
            return 1 if b != 0 else 0
        elif isinstance(b, float):
            r = (1 if b != 0.0 else 0) if not math.isnan(b) else nan
            return r
        else:
            return b

    @classmethod
    def parse_int(cls, i: str) -> int:
        """
        Parses an int string
        :param str i: int string
        :return: parsed int
        :rtype: int
        """
        if isinstance(i, str):
            return int(i) if len(i) > 0 else nan
        elif isinstance(i, bool):
            return 1 if i else 0
        elif isinstance(i, float):
            return int(i) if not math.isnan(i) else nan
        else:
            return i

    @classmethod
    def parse_string(cls, i: str) -> str:
        """
        Resolves string nans
        :param str i:  string
        :return: parsed int
        :rtype: str
        """
        if i is None or pd.isna(i):
            return nan
        elif isinstance(i, float):
            return str(i) if not math.isnan(i) else nan
        elif isinstance(i, str):
            return i if len(i) > 0 else nan
        else:
            return str(i)

    @classmethod
    def remove_sql_comments(cls, sql_text: str) -> str:
        """
        Remove SQL comments from a SQL text
        :param str sql_text: SQL texts with comments
        :return: SQL texts without comments
        :rtype: str
        """
        pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
        # first group captures quoted strings (double or single)
        # second group captures comments (//single-line or /* multi-line */)
        regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

        def _replacer(match):
            # if the 2nd group (capturing comments) is not None,
            # it means we have captured a non-quoted (real) comment string.
            if match.group(2) is not None:
                return ""  # so we will return empty to remove the comment
            else:  # otherwise, we will return the 1st group
                return match.group(1)  # captured quoted-string

        no_comments = regex.sub(_replacer, sql_text)
        # replace multiple newlines with one
        return re.sub(r'\n+', '\n', no_comments).strip()

    @classmethod
    def datatype_to_gbq_datatype(cls, datatype: str) -> str:
        """
        Converts SQL datatype to Google BigQuery datatype
        :param str datatype: SQL datatype
        :return: BigQuery datatype name
        :rtype: str
        """
        if datatype.lower().startswith(('char', 'varchar', 'text')):
            return 'STRING'
        elif datatype.lower().startswith(('decimal', 'numeric')):
            return 'FLOAT'
        elif datatype.lower().startswith(('datetime')):
            return 'DATETIME'
        elif datatype.lower().startswith(('timestamp')):
            return 'TIMESTAMP'
        else:
            return datatype.upper()
