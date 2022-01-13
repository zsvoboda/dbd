# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

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
