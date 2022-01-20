# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

# Configuring MySQL database and connection
Check out [the MySQL examples README](../README.md) for more information on MySQL examples setup. 

# Running the example
Use the following commands to run the COVID CZ example:

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/mysql/covid_cz
dbd run . 
```

Once you run these commands, you can review the newly created tables in the `public` MySQL database.
