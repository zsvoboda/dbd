# MySQL dbd examples
The MySQL examples require access to a MySQL database server. You can install MySQL locally 
or provision a hosted MySQL service (e.g. AWS RDS or GCP CLoudSQL).  

# Configuring MySQL database connection
The `dbd.profile` configuration file contains the following MySQL connection URL:

`mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4`

Please make sure that your environment contains the environment variables above. For example:

```shell
export MYSQL_USER=demouser
export MYSQL_PASSWORD=demopass
export MYSQL_HOST=localhost
export MYSQL_DB=public
```

You also need to create a new `covid_cz` MySQL database and `demouser` user that can create new tables in this database.

# Running MySQL examples
Use the following commands to run a selected MySQL example:

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/mysql/covid_cz
dbd --profile=../dbd.profile run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` MySQL database.
