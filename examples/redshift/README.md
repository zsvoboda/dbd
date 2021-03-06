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
export REDSHIFT_DB=covid_cz
```

You also need to create a new `covid_cz` Redshift database and `demouser` user that can create new tables in this database.

# Configuring Redshift fast loading mode
Fast loading mode copies data to Redshift using COPY command that is much faster than the traditional INSERT command. 
To enable fast loading mode, you need specify `copy_stage` parameter in the `dbd.project` configuration file. 
The `copy_stage` parameter must reference a storage definition in your `dbd.profile` configuration file.
Check the example configuration files in the `examples/redshift/covid_cz` directory. Here are the example definitions of the 
environment variables that these configuration files use:

```shell
export AWS_COVID_STAGE_S3_URL="s3://covid/stage"
export AWS_COVID_STAGE_S3_ACCESS_KEY="AKIA43SWERQGXMUYFIGMA"
export AWS_COVID_STAGE_S3_S3_SECRET_KEY="iujI78eDuFFGJF6PSjY/4CIhEJdMNkuS3g4t0BRwX"
```

# Running the example
Use the following commands to run the COVID CZ example:

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/redshift/covid_cz
dbd run . 
```

Once you run these commands, you can review the newly created tables in the `covid_cz` Redshift database.

# Examples

This directory contains the following examples:

* [Basic example](basic/README.md) demonstrates simple `dbd` usage. It loads couple CSV files with default datatypes and performs a simple SQL transformation.
* [COVID](covid/README.md) shows loading of an online `.csv` file.
* [COVID CZ](covid_cz/README.md) is the most complex example that loads data from online CSV files from the Czech Republic's Ministry of Health and transform them to analytical model with constraints and referential integrity.
