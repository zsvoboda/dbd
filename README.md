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

DBD also requires another `.dbd.profile` configuration file that contains connections to your databases. 
This file is located in the current directory or in your home directory.   




