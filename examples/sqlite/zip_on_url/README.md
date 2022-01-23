# Referencing data files in an online ZIP archive
This example loads 1 CSV file from a `covid-variants.csv` located in a ZIP file on `https://raw.githubusercontent.com/zsvoboda/dbd/master/tests/fixtures/capabilities/zip_local/data/archive.zip`
and writes it to an `omicron` table in a SQLite database. Take a look at the way how a file inside the zip file is referenced 
using the `>` path separator in the `model/omicron.ref` file:

`https://raw.githubusercontent.com/zsvoboda/dbd/master/tests/fixtures/capabilities/zip_local/data/archive.zip>covid-variants.csv`

The `model/omicron.yaml` file overrides the default `omicron` table's columns datatypes.  

The `model/omicron.yaml` file also specifies that a new table will be created (vs. a view) and that
the table is dropped if it exists before it is created.

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/zip_on_url
dbd run . 
```

These commands create a new `zip_on_url.db` SQLite database with the table that is created and loaded 
from the file in the `model` directory.
