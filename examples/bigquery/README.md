# Google BigQuery dbd examples
The BigQuery examples require access to a Google BigQuery database service. 
Read [Google BigQuery documentation](https://cloud.google.com/bigquery/docs) 
for more information on its provisioning.

# Configuring BigQuery database connection
The `dbd.profile` configuration file contains the following BigQuery connection URL:

`bigquery://{{BIGQUERY_PROJECT}}/{{BIGQUERY_DATASET}}`

Please make sure that your environment contains the environment variables above. For example:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/Users/me/.config/gcloud/application_default_credentials.json
export BIGQUERY_PROJECT=myproject
export BIGQUERY_DATASET=demo
```

The `GOOGLE_APPLICATION_CREDENTIALS` points to a file that contains the credentials for the BigQuery service.
Read this [documentation](https://googleapis.dev/python/google-api-core/latest/auth.html) for more information on how to 
obtain the credentials.

# Running BigQuery examples
Use the following commands to run a selected BigQuery example:

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/bigquery/covid_cz
dbd --profile=../dbd.profile run . 
```

Once you run these commands, you can review the newly created tables in the BigQuery dataset.


