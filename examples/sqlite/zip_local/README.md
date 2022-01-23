# Referencing data files in a local ZIP archive
This example loads 1 CSV file from the local zip file located in the data directory and writes it to an 
`omicron` table in a SQLite database. Take a look at the way how a file inside the zip file is referenced 
using the `>` path separator in the `model/omicron.ref` file.

The `model/omicron.yaml` file overrides the default `omicron` table's columns datatypes.  

The `model/omicron.yaml` file also specifies that a new table will be created (vs. a view) and that
the table is dropped if it exists before it is created.

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/zip_local
dbd run . 
```

These commands create a new `zip_local.db` SQLite database with the table that is created and loaded 
from the file in the `model` directory.
