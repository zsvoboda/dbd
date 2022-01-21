# COVID US example
This example loads 3 local data files to the database:

* `area.csv` 
* `population.csv`
* `state.csv`

Then it also loads 1 CSV file from `https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv` 
and writes it to a `us_covid` table in a SQLite database. The url is defined in the `model/u_covid.ref` file.

Then the data from the tables above are transformed and loaded to the `us_states` and `us_states_covid` tables 
using the SQL statements in the `model/us_states.sql` and `model/us_states_covid.sql` files. 
The `model/us_states.yaml` and `model/us_states_covid.yaml` files are used to override the 
default `us_states` and `us_states_covid` tables column's data types and adds primary key, nullability 
constraints, and index.

The `yaml` files also specify that dbd creates new tables (vs. views) and that the tables are dropped 
if they exists before they are created.

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/covid_us
dbd run . 
```

These commands create a new `covid_us.db` SQLite database with tables that are created and loaded 
from the corresponding files in the `model` directory.

More detailed tutorial on [this example can be found here](https://zsvoboda.medium.com/analyze-covid-data-in-less-than-5-minutes-9176f440dd1a)
