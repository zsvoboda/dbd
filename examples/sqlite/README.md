# SQLite examples
This directory contains example dbd models for multiple databases. 
The SQLite examples are great starting point because they don't require any setup. 
You don't need to install any database server, create database or setup any database connection. 

This directory contains the following examples:

* [Basic example](examples/basic/README.md) demonstrates simple `dbd` usage. It loads couple CSV files with default datatypes and performs a simple SQL transformation.
* [COVID CZ example](examples/covid_cz/README.md) is the most complex example that loads data from online CSV files from Czech Republic's Ministry of Health and transform them to analytical model with constraints and referential integrity.
* [COVID US example](examples/covid_us/README.md) shows loading files from url, and local files. It also contains simple constraints (nullability) and indexes.
* [Data formats example](examples/data_formats/README.md) demonstrates loading of local `.json`, `.xlsx`, and `.parquet` files. You can also learn how to setup referential integrity and other table constraints.  
* [Jinja2 templates example](examples/jinja_template/README.md) shows how to load online and local files. The `.ref` file contains [Jinja2](https://jinja.palletsprojects.com/en/3.0.x/) macro that is expanded before the model execution.
* [REF file example](examples/ref_file/README.md) shows loading of an online `.csv` file.


