# REF file example
This example loads 1 CSV file from `https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv` 
and writes it to a `covid` table in a Snowflake database. The url is defined in the `model/covid.ref` file.

The `model/covid.yaml` file overrides the default `covid` table's columns datatypes.  

The `model/covid.yaml` file also specifies that a new table will be created (vs. a view) and that
the table is dropped if it exists before it is created.

# Configuring Snowflake database and connection
Check out [the Snowflake examples README](../README.md) for more information on Snowflake examples setup. 

# Running the example
Use the following commands to run the COVID example:

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/snowflake/covid
dbd run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` Snowflake database.
