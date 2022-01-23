# SQLite examples
This directory contains example dbd models for multiple databases. 
The SQLite examples are great starting point because they don't require any setup. 
You don't need to install any database server, create database or setup any database connection. 

This directory contains the following examples:

* [Basic example](basic/README.md) demonstrates simple `dbd` usage. It loads couple CSV files with default datatypes and performs a simple SQL transformation.
* [COVID CZ example](covid_cz/README.md) is the most complex example that loads data from online CSV files from the Czech Republic's Ministry of Health and transform them to analytical model with constraints and referential integrity.
* [COVID US example](covid_us/README.md) shows loading files from url, and local files. It also contains simple constraints (nullability) and indexes.
* [Data formats example](data_formats/README.md) demonstrates loading of local `.json`, `.xlsx`, and `.parquet` files. You can also learn how to setup referential integrity and other table constraints.  
* [Jinja2 templates example](jinja_template/README.md) shows how to load online and local files. The `.ref` file contains [Jinja2](https://jinja.palletsprojects.com/en/3.0.x/) macro that is expanded before the model execution.
* [REF file example](ref_file/README.md) shows loading of an online `.csv` file.
* [Referencing data files in a local ZIP archive](zip_local/README.md) shows loading of a `.csv` file located in a local ZIP archive.
* [Referencing data files in an online ZIP archive](zip_on_url/README.md) shows loading of a `.csv` file located in an online ZIP archive.
* [Referencing files in Kaggle datasets](zip_on_kaggle/README.md) shows loading of a `.csv` file located in a Kaggle dataset.


