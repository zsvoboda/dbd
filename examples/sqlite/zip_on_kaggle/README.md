# Referencing files in Kaggle datasets
This example loads 3 CSV files from the [kalilurrahman/new-york-times-covid19-dataset](https://www.kaggle.com/kalilurrahman/new-york-times-covid19-dataset?select=us-counties.csv) 
Kaggle dataset and writes them to tables in a SQLite database. Take a look at the way how te dataset files are referenced 
using the `>` path separator in the `.ref` files in the `model` directory.

For example:

`kaggle://kalilurrahman/new-york-times-covid19-dataset>us.csv`

Take a lok at the [basic](../basic/README.md) example to see how you can override column's data types.

# Kaggle authentication
To use the Kaggle API, sign up for a Kaggle account at https://www.kaggle.com. Then go to the 'Account' tab 
of your user profile (`https://www.kaggle.com/<username>/account`) and select 'Create API Token'. 
This will trigger the download of kaggle.json, a file containing your API credentials. 
Place this file in the location `~/.kaggle/kaggle.json` (on Windows in the location 
`C:\Users\<Windows-username>\.kaggle\kaggle.json` - you can check the exact location, 
sans drive, with `echo %HOMEPATH%`). 
You can define a shell environment variable `KAGGLE_CONFIG_DIR` to change this location to `$KAGGLE_CONFIG_DIR/kaggle.json` 
(on Windows it will be `%KAGGLE_CONFIG_DIR%\kaggle.json`).

For your security, ensure that other users of your computer do not have read access to your credentials. 
On Unix-based systems you can do this with the following command:

`chmod 600 ~/.kaggle/kaggle.json`

You can also choose to export your Kaggle username and token to the environment:

```shell
export KAGGLE_USERNAME=datadinosaur
export KAGGLE_KEY=xxxxxxxxxxxxxx
```

# Running the demo 

```shell
python3 -m venv dbd-env
source dbd-env/bin/activate
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/zip_on_kaggle
dbd run . 
```

These commands create a new `zip_on_kaggle.db` SQLite database with the tables that is created and loaded 
from the files in the `model` directory.
