# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

# Configuring Snowflake database connection
The `dbd.profile` configuration file contains the following Snowflake connection URL:

`snowflake://{{ SNOWFLAKE_USER }}:{{ SNOWFLAKE_PASSWORD }}@{{ SNOWFLAKE_ACCOUNT_IDENTIFIER }}/{{ SNOWFLAKE_DB }}/{{ SNOWFLAKE_SCHEMA }}?warehouse={{SNOWFLAKE_WAREHOUSE }}`

Please make sure that your environment contains the environment variables above. For example:

```shell
export SNOWFLAKE_USER=demouser
export SNOWFLAKE_PASSWORD=demopass
export SNOWFLAKE_ACCOUNT_IDENTIFIER=account
export SNOWFLAKE_DB=covid_cz
export SNOWFLAKE_SCHEMA=PUBLIC
export SNOWFLAKE_WAREHOUSE=MY_DWH
```

You also need to create a new `covid_cz` Snowflake database and `demouser` user that can create new tables in this database.

# Running the example
Use the following commands to run the COVID CZ example:

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/snowflake/covid_cz
dbd --profile=../dbd.profile run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` Snowflake database.
