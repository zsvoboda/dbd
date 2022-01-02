# DBD - framework for declarative database definition
DBD framework allows you to define your database schema and content 
declaratively. Database is represented by a hierarchy of directories and
files stored in your DBD project files. 

## TLDR

1. `dbd init my-first-project`
2. `cd my-first-project`
3. `echo "databases:\n  states:\n    db.url: 'sqlite:///my-sqllite.db'" > .dbd.profile`
4. `dbd validate .`
5. `dbd run .`
6. Connect to the newly created `my-sqllite.db` database and review `area`, `population`, and `state` tables that have been created from the files in the `model` directory.

Now you can delete the example files from the `model` directory, copy your Excel, JSON, or CSV files there instead. 
Then execute `dbd run .` and your files should get loaded into the database.

You can also create a YAML configuration files for your Excel, JSON, or CSV files for specifying individual column's
data type, index or constraint (e.g. primary key, foreign key, or check).

Later you can create an SQL file that performs SQL insert-from-select to transform the loaded data.

## Install DBD
DBD requires Python 3.7.1 or higher. 

### PyPI

```shell
pip3 install dbd
```

OR

```shell
git clone https://github.com/zsvoboda/dbd.git
cd dbd
pip3 install .
```

### Poetry

```shell
git clone https://github.com/zsvoboda/dbd.git
cd dbd
poetry install
``` 

## Generate a new DBD project
You can generate initial layout of your DBD project using the `init` command:

```shell
dbd init <new-project-name>
```

The `init` command generates a new DBD project directory with the following content: 

- `model` directory that contains the content files. dbd supports files with `.sql`, `.ddl`, `.csv`, `.json`, `.xlsx` and other extensions.  
- `dbd.project` project configuration file 

DBD also requires `.dbd.profile` configuration file that contains connections to your databases. 
This file is located in the current directory or in your home directory.  

## DBD profile configuration file
DBD stores database connections in a profile configuration file. It searches for `.dbd.profile` file in the current or in 
your home directory. You can always specify a custom profile file location using the `--profile` option of the `dbd` command. 

The profile file is YAML file with the following structure:

```yaml
databases:
  states:
    db.url: <sql-alchemy-database-url>
```

Read more about [SQL Alchemy database URLs here](https://docs.sqlalchemy.org/en/14/core/engines.html). 

The profile file can contain Jinja2 macros that substitute your environment variables. For example, you can reference 
database password stored in a `$SQLITE_PASSWORD` environment variable via `{{ SQLITE_PASSWORD }}` in your DBD profile.

## DBD project configuration file
DBD stores project configuration in a project configuration file that is usually stored in your DBD project directory. 
DBD searches for `dbd.project` file in the current directory. You can also use the `--project` option pf the `dbd` 
command to specify a custom project configuration file. 

The project configuration file also uses YAML format and references the DBD model directory with the `.sql`, `.csv` 
and other supported files. It also references the database configuration from the profile config file. For example:

```yaml
model: model
database: states
```

Similarly like the profile file, you can use the environment variables substitution in the procet config file too 
(e.g. `{{ SQLITE_DB_NAME }}`).

## Model directory
The model directory contains directories and DBD files. Each subdirectory of the model directory represents 
a database schema. For example, this model directory structure

```text
dbd-project-directory
+- schema1
 +-- us_states.csv
+- schema2
 +-- us_counties.csv
```

creates two database schemas: `schema1` and `schema2` and creates two database tables: `us_states` in `schema1` 
and `us_counties` in `schema2`. Both tables are populated with the data from the CSV files.  

The following file types are supported:

- __DATA files:__ `.csv`, `.json`, `.xls`, `.xlsx`, `.parquet` files are loaded to the database as tables
- __SQL files:__ with SQL SELECT statements are executed using insert-from-select SQL construct. The INSERT command is generated (the SQL file only contains the SQL SELECT statement)
- __DDL files:__ contain a sequence of SQL statements separated by semicolon. The DDL files can be named `prolog.ddl` and `epilog.ddl`. The `prolog.ddl` is executed before all other files in a specific schema. The `epilog.ddl` is executed last. The `prolog.ddl` and `epilog.ddl` in the top-level model directory are executed as the very first and tne very last files in the model. 
- __YAML files:__ specify additional configuration to the __DATA__ and __SQL__ files. 

## YAML model files
YAML file specify additional configuration for a corresponding __DATA__ or __SQL__ file with the same base file name.
here is an `area.csv` YAML configuration example:

```yaml
table:
  columns:
    state_name:
      nullable: false
      index: true
      primary_key: true
      foreign_keys:
        - state.state_name
      type: VARCHAR(50)
    area_sq_mi:
      nullable: false
      index: true
      type: INTEGER
process:
  materialization: table
  mode: drop
```

### Table section
YAML file's columns are mapped to the `area.csv` data file columns by the column name. 
The following column parameters are supported:

- __type:__ column's SQL type
- __primary_key:__ is the column part of table's primary key (true|false)?
- __foreign_keys:__ all other database table columns that are referenced from a column in <table>.<column> format
- __nullable:__ does column allow null values (true|false)?
- __index:__ is column indexed (true|false)?
- __unique:__ does column store unique values (true|false)?

### Process section
The `process` section specifies the following processing options:

- __materialization:__ specifies whether DBD creates a physical `table` or a `view` when processing a SQL file.
- __mode:__ specifies how DBD works with a table. You can specify values `drop`, `truncate`, or `keep`. The  __mode__ option is ignored for views.




