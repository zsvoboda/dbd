# dbd examples
The easiest way how to execute them is to either clone or download dbd's github repository and start with the SQLite examples.

```shell
pip3 install dbd
git clone https://github.com/zsvoboda/dbd.git
cd dbd/examples/sqlite/basic
dbd --profile=../dbd.profile run . 
```

These commands should create a new `basic.db` SQLite database with `area`, `population`, and `state` tables that are created and loaded from the corresponding files in the `model` directory.
