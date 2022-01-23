# REF file example
This example loads 1 CSV file from `https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv` 
and writes it to a `covid` table in a SQLite database. The file reference is in the `model/covid.ref` file.

The `model/covid.yaml` file overrides the default `covid` table's columns datatypes.  

The `model/covid.yaml` file also specifies that a new table will be created (vs. a view) and that
the table is dropped if it exists before it is created.

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/ref_file
dbd run . 
```

These commands create a new `ref_file.db` SQLite database with the table that is created and loaded 
from the file in the `model` directory.
