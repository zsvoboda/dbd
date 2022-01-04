# DBD - tool for declarative database definition
DBD tool allows you to define your database schema and content declaratively. Database is represented by a 
hierarchy of directories and files stored in a model directory. 

![How DBD works](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/dbd.infographic.png)

DBD is great for declarative creation of any database. It is particularly designed for ELT (Extract/Load/Transform) 
data pipelines used in data analytics and data warehousing. 

## TLDR: Whetting Your Appetite

1. `dbd init test`
2. `cd test`
3. Check out files in the `model` directory.  
4. `dbd validate .` 
5. `dbd run .`
6. Connect to the newly created `states.db` sqlite database and review `area`, `population`, and `state` tables that have been created from the files in the `model` directory.

Now you can delete the example files from the `model` directory, copy your Excel, JSON, or CSV files there instead. 
Then execute `dbd run .` again. Your files should be loaded in the `states.db` database.

You can create a YAML configuration files for your data (Excel, JSON, or CSV) files to specify individual column's
data types, indexes or constraints (e.g. primary key, foreign key, or check). See below for more details. 

You can also add an SQL file that performs insert-from-select SQL statement to create database tables with transformed data.

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
You can generate DBD project initial layout by executing `init` command:

```shell
dbd init <new-project-name>
```

The `init` command generates a new DBD project directory with the following content: 

- `model` directory that contains the content files. dbd supports files with `.sql`, `.ddl`, `.csv`, `.json`, `.xlsx` and other extensions.  
- `dbd.profile` configuration file that specifies database connections 
- `dbd.project` project configuration file

## DBD profile configuration file
DBD stores database connections in the `dbd.profile` configuration file. DBD searches for `dbd.profile` file in current or in 
your home directory. You can always specify a custom profile file location using the `--profile` option of the `dbd` command. 

The profile file is YAML file with the following structure:

```yaml
databases:
  states:
    db.url: <sql-alchemy-database-url>
```

Read more about [SQL Alchemy database URLs here](https://docs.sqlalchemy.org/en/14/core/engines.html). 

The profile file can contain Jinja2 macros that substitute your environment variables. For example, you can reference 
database password stored in a `SQLITE_PASSWORD` environment variable via `{{ SQLITE_PASSWORD }}` in your DBD profile.

## DBD project configuration file
DBD stores project configuration in a project configuration file that is usually stored in your DBD project directory. 
DBD searches for `dbd.project` file in your project's directory root. You can also use the `--project` option of the `dbd` 
command to specify a custom project configuration file. 

The project configuration file also uses YAML format and references the DBD model directory with the `.sql`, `.csv` 
and other supported files. It also references the database configuration from the profile config file. For example:

```yaml
model: model
database: states
```

Similarly like the profile file, you can use the environment variables substitution in the project config file too 
(e.g. `{{ SQLITE_DB_NAME }}`).

## Model directory
Model directory contains directories and DBD files. Each subdirectory of the model directory represents 
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

DBD supports following files located in the `model` directory:

* __DATA files:__ `.csv`, `.json`, `.xls`, `.xlsx`, `.parquet` files are loaded to the database as tables
* __SQL files:__ with SQL SELECT statements are executed using insert-from-select SQL construct. The INSERT command is generated (the SQL file only contains the SQL SELECT statement)
* __DDL files:__ contain a sequence of SQL statements separated by semicolon. The DDL files can be named `prolog.ddl` and `epilog.ddl`. The `prolog.ddl` is executed before all other files in a specific schema. The `epilog.ddl` is executed last. The `prolog.ddl` and `epilog.ddl` in the top-level model directory are executed as the very first and tne very last files in the model. 
* __YAML files:__ specify additional configuration to the __DATA__ and __SQL__ files.

## SQL files 
SQL file performs SQL transformation within your database. It contains a SQL SELECT statement that DBD wraps in 
insert-from-select statement, executes it, and stores the result into a table or view that inherits its name from the 
SQL file name.

Here is an example of `us_states.sql` file that creates a new `us_states` database table.

```sqlite
SELECT
        state.abbrev AS state_code,
        state.state AS state_name,
        population.population AS state_population,
        area.area_sq_mi  AS state_area_sq_mi
    FROM state
        JOIN population ON population.state = state.abbrev
        JOIN area on area.state_name = state.state
```

## YAML files
YAML file specify additional configuration for a corresponding __DATA__ or __SQL__ file with the same base file name.
Here is a YAML configuration example for the `us_states.sql` file above:

```yaml
table:
  columns:
    state_code:
      nullable: false
      primary_key: true
      type: CHAR(2)
    state_name:
      nullable: false
      index: true
      type: VARCHAR(50)
    state_population:
      nullable: false
      type: INTEGER
    state_area_sq_mi:
      nullable: false
      type: INTEGER
process:
  materialization: table
  mode: drop
```

Note that we re-type the `state_population` and the `state_area_sq_mi` columns to INTEGER, disallow
NULL values in all columns, and specify that the `state_code` column is table's primary key.

The table is dropped and data re-loaded in full everytime the dbd executes this model. 

### Table section
YAML file's columns are mapped to a columns of the table that DBD creates from a corresponding DATA or SQL file. 
For example, a CSV header or SQL SELECT column `AS` clause. You can specify the following column's parameters:

* __type:__ column's SQL type.
* __primary_key:__ is the column part of table's primary key (true|false)?
* __foreign_keys:__ all other database table columns that are referenced from a column in table.column format
* __nullable:__ does column allow null values (true|false)?
* __index:__ is column indexed (true|false)?
* __unique:__ does column store unique values (true|false)?

### Process section
The `process` section specifies the following processing options:

* __materialization:__ specifies whether DBD creates a physical `table` or a `view` when processing a SQL file.
* __mode:__ specifies how DBD works with a table. You can specify values `drop`, `truncate`, or `keep`. The  __mode__ option is ignored for views.

## License
DBD code is open-sourced under [BSD 3-clause license](LICENSE). 

## Resources and References
- [DBD github repo](https://github.com/zsvoboda/dbd)
- [DBD PyPi](https://pypi.org/project/dbd/)
- [Submit issue](https://github.com/zsvoboda/dbd/issues)
