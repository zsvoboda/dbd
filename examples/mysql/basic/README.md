# Basic example
This example loads 3 data files to the database:

* `area.csv` 
* `population.csv`
* `state.csv`

with default datatypes (there are no yaml configuration files that would override data types, add 
referential integrity or other constraints). 

Then the data from the tables above are transformed and loaded to the `us_states` table using the 
SQL statement in the `model/us_states.sql` file. The `model/us_states.yaml` file is used to override 
the default `us_states` table column's data types and adds primary key, nullability constraints, 
and index.

The `model/us_states.yaml` file also specifies that a new table will be created (vs. a view) and that
the table is dropped if it exists before it is created.

# Configuring MySQL database and connection
Check out [the MySQL examples README](../README.md) for more information on MySQL examples setup. 

# Running the example
Use the following commands to run the basic example:

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/mysql/basic
dbd run . 
```

Once you run these commands, you can review the newly created tables in the `public` MySQL database.
