# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

# Creating and loading SQLite database
```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/covid_cz
dbd --profile=../dbd.profile run . 
```

These commands create a new `covid_cz.db` SQLite database with tables that are created and loaded from the corresponding files in the `model` directory.
