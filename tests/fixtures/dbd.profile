databases:
  states:
    db.url: "sqlite:///tmp/basic.db"
  data_formats:
    db.url: "sqlite:///tmp/data_formats.db"
  invalid_model:
    db.url: "sqlite:///tmp/invalid_model.db"
  screwed:
    db.url: "sqlite:///tmp/screwed.db"
  covid_ref:
    db.url: "sqlite:///tmp/covid_ref.db"
  covid_cz:
    db.url: "postgresql://demouser:demopass@localhost/covid_cz"
  test_bigquery:
    db.url: "bigquery://{{BIGQUERY_PROJECT}}/{{BIGQUERY_DATASET}}"
  test_snowflake:
    db.url: "snowflake://{{ SNOWFLAKE_USER }}:{{ SNOWFLAKE_PASSWORD }}@{{ SNOWFLAKE_ACCOUNT_IDENTIFIER }}/{{ SNOWFLAKE_DB }}/{{ SNOWFLAKE_SCHEMA }}?warehouse={{SNOWFLAKE_WAREHOUSE }}"
  test_sqlite:
    db.url: "sqlite:///tmp/test_sqlite.db"
  test_postgres:
    db.url: "postgresql://demouser:demopass@localhost/covid_cz"
  test_mysql:
    db.url: "mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4"
