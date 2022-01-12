# SQLite dbd examples
The SQLite examples are very easy to run as they don't require any database server installation or connection. 
You can use the following commands to run them.

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/covid_cz
dbd --profile=../dbd.profile run . 
```

These commands create a new `covid_cz.db` SQLite database with tables that are created and loaded from the corresponding 
files in the `model` directory.

