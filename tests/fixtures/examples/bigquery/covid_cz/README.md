# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

# Configuring BigQuery database and connection
Check out [the BigQuery examples README](../README.md) for more information on BigQuery examples setup. 

# Running BigQuery examples
Use the following commands to run a selected BigQuery example:

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/bigquery/covid_cz
dbd run . 
```

Once you run these commands, you can review the newly created tables in the BigQuery dataset.
