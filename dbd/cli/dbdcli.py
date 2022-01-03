import importlib.metadata
import os
import shutil
from typing import Dict, Any, List

import click
from sqlalchemy import text

from dbd.config.dbd_profile import DbdProfile
from dbd.config.dbd_project import DbdProject
from dbd.executors.model_executor import ModelExecutor

this_script_dir = os.path.dirname(__file__)


class DbdException(Exception):
    pass


class Dbd(object):
    """
    Top level CLI object
    """

    def __init__(self, debug: bool = False):
        """
        Constructor
        :param bool debug: debug flag
        """
        self.__debug = debug

    def debug(self) -> bool:
        """
        Debug flag getter
        :return: debug flag
        :rtype: bool
        """
        return self.__debug


def print_version(ctx, param, value):
    """
    Prints DBD version
    """
    click.echo(f"You're using DBD version {importlib.metadata.version('dbd')}.")
    ctx.exit(0)


@click.group()
@click.option('--debug/--no-debug', envvar='DBD_DEBUG', default=False, help='Sets debugging on/off')
@click.option(
    '--version',
    help="Print the DBD version and exit.",
    is_flag=True,
    expose_value=False,
    is_eager=True,
    callback=print_version
)
@click.pass_context
def cli(ctx, debug, version):
    ctx.obj = Dbd(debug)


@cli.command(help='Initializes a new DBD project.')
@click.argument('dest', required=False, default='my_new_dbd_project')
@click.pass_obj
def init(dbd, dest):
    src = os.path.join(this_script_dir, '..', 'resources', 'template')
    if os.path.exists(dest):
        raise DbdException(f"Can't overwrite directory '{dest}'")
    shutil.copytree(src, dest)
    click.echo(f"New project {dest} generated. Do cd {dest}; dbd run .")


@cli.command(help='Executes project.')
@click.argument('dest', required=False, default='.')
@click.option('--profile', envvar='DBD_PROFILE', default='dbd.profile', help='Profile configuration file')
@click.option('--project', envvar='DBD_PROJECT', default='dbd.project', help='Project configuration file')
@click.pass_obj
def run(dbd, profile, project, dest):
    prf = DbdProfile.load(os.path.join('.', profile))
    prj = DbdProject.load(prf, os.path.join(dest, project))
    model = ModelExecutor(prj)
    engine = prj.alchemy_engine_from_project()
    model.execute(engine)


@cli.command(help='Validates project.')
@click.argument('dest', required=False, default='.')
@click.option('--profile', envvar='DBD_PROFILE', default='dbd.profile', help='Profile configuration file')
@click.option('--project', envvar='DBD_PROJECT', default='dbd.project', help='Project configuration file')
@click.pass_obj
def validate(dbd, profile, project, dest):
    prf = DbdProfile.load(os.path.join('.', profile))
    prj = DbdProject.load(prf, os.path.join(dest, project))
    model = ModelExecutor(prj)
    engine = prj.alchemy_engine_from_project()
    try:
        engine.execute(text("SELECT 1"))
    except Exception:
        click.echo(
            f"Can't connect to the target database. Check profile configuration in "
            f"'{os.path.normpath(os.path.join(dest, profile))}'.")
    validation_result, validation_errors = model.validate()
    if validation_result:
        click.echo("No errors found. Model is valid.")
    else:
        click.echo("Model isn't valid. Please fix the following errors:")
        __echo_validation_errors(validation_errors)


def __echo_validation_errors(validation_errors: Dict[str, Any]):
    """
    Top level function for printing validation errors
    :param validation_errors:
    :return:
    """
    __echo_validation_level(validation_errors)


class InvalidValidationErrorStructure(Exception):
    pass


def __echo_validation_level(level_validation_errors: Dict[str, Any], indent: int = 0):
    """
    Echo validation error line (called recursively on all Dict values)
    :param level_validation_errors: Dict with validation result
    :param indent: indentation level
    """
    for (k, v) in level_validation_errors.items():
        if isinstance(v, str):
            msg = f"{k}:{v}"
            click.echo(msg.rjust(indent * 2 + len(msg), ' '))
        elif isinstance(v, Dict):
            msg = f"{k}:"
            click.echo(msg.rjust(indent * 2 + len(msg), ' '))
            __echo_validation_level(v, indent + 1)
        elif isinstance(v, List):
            msg = f"{k}:{str(v)}"
            click.echo(msg.rjust(indent * 2 + len(msg), ' '))
        else:
            raise InvalidValidationErrorStructure(f"Invalid validation result: '{v}' isn't supported type.")
