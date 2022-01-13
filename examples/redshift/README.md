# AWS Redshift dbd examples
The Snowflake examples require access to AWS Redshift database service. 
Read [Redshift documentation](https://aws.amazon.com/redshift/free-trial/) 
for more information on its provisioning.

# Configuring Redshift database connection
The `dbd.profile` configuration file contains the following Redshift connection URL:

`redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}`

Please make sure that your environment contains the environment variables above. For example:

```shell
export REDSHIFT_USER=demouser
export REDSHIFT_PASSWORD=demopass
export REDSHIFT_HOST="redshift-cluster-1.cw0tdqmscsfp.us-east-2.redshift.amazonaws.com"
export REDSHIFT_PORT=5439
export REDSHIFT_DB=dev

```

You also need to create a new `covid_cz` Redshift database and `demouser` user that can create new tables in this database.

# Running the example
Use the following commands to run the COVID CZ example:

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/redshift/covid_cz
dbd --profile=../dbd.profile run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` Redshift database.
