# Postgres dbd examples
The Postgres examples require access to a Postgres database server. You can install Postgres locally 
or provision a hosted Postgres service (e.g. AWS RDS or GCP CLoudSQL).  

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

# Running Postgres examples
Use the following commands to run a selected Postgres example:

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/postgres/covid_cz
dbd --profile=../dbd.profile run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` Postgres database.


