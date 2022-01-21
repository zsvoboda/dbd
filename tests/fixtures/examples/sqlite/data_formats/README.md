# Data formats example
This example loads 3 data files to the database:

* `area.json` 
* `population.xlsx`
* `state.parquet`

with datatypes specified in respective yaml files. 

Then the data from the tables above are transformed and loaded to the `total_population_2k` and `under18_population_2k`
tables using the SQL statements in the `model/total_population_2k.sql` and `model/under18_population_2k.sql` files. 
The corresponding yaml file are used to override the default table's column's data types and adds primary key, 
nullability constraints, indexes, and foreign keys. Check out the foreign key definitions in the the 
`model/total_population_2k.yaml` and `model/under18_population_2k.yaml` files.

THe `prolog.ddl` file is executed first to setup the SQLite database.

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/data_formats
dbd run . 
```

These commands create a new `data_formats.db` SQLite database with tables that are created and loaded 
from the corresponding files in the `model` directory. 
