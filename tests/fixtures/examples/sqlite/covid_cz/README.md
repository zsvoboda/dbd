# Czech Republic COVID-19 data 
This example downloads the complete COVID-19 tracking data from the [Czech Ministry of Health site](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) 
and transform it to a simple analytical data model.  

![COVID CZ data model](https://raw.githubusercontent.com/zsvoboda/dbd/master/img/covid.cz.datamodel.png)

This example is more complex and uses the following files:

- `ext_*.ref` files reference CSV files published by the Czech Ministry of Health 
- `ext_*.yaml` files define the column data types of the CSV files above
- `*.sql` files contain insert-from-select SQL statements that transform and load data from the ext tables above
- Corresponding `*.yaml` files define the column data types of the tables that the `*.sql` files create. These yaml files also contain primary keys, foreign keys, and other constraints

# Running the example

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/covid_cz
dbd run .  
```

These commands create a new `covid_cz.db` SQLite database with tables that are created and 
loaded from the corresponding files in the `model` directory.
