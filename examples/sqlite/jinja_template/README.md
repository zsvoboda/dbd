# Jinja2 templates example
This example loads 4 data files to a single `jinja` table to a SQLite database.
The `model/jinja.ref` file is a Jinja2 template file that uses the `{% for %}` macro to load data 
from 4 different URLs. Then `dbd` also loads two local files from the `data` directory. The data 
files are referenced using relative paths to the `.ref` file.

The `model/jinja.yaml` file is used to override the default `jinjs` table column's data types.

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/jinja_template
dbd run . 
```

These commands create a new `jinja.db` SQLite database with tables that are created and loaded 
from the corresponding files in the `model` directory. 
