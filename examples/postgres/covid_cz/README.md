# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

# Configuring Postgres database connection
The `dbd.profile` configuration file contains the following Postgres connection URL:

`postgresql://{{ POSTGRES_USER }}:{{ POSTGRES_PASSWORD }}@{{ POSTGRES_HOST }}/{{ POSTGRES_DB }}`

Please make sure that your environment contains the environment variables above. For example:

```shell
export POSTGRES_USER=demouser
export POSTGRES_PASSWORD=demopass
export POSTGRES_HOST=localhost
export POSTGRES_DB=covid_cz
```

You also need to create a new `covid_cz` Postgres database and `demouser` user that can create new tables in this database.

# Running the example
Use the following commands to run the COVID CZ example:

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/postgres/covid_cz
dbd --profile=../dbd.profile run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` Postgres database.
