databases:
  test_bigquery:
    db.url: "bigquery://{{BIGQUERY_PROJECT}}/{{BIGQUERY_DATASET}}"
  test_snowflake:
    db.url: "snowflake://{{ SNOWFLAKE_USER }}:{{ SNOWFLAKE_PASSWORD }}@{{ SNOWFLAKE_ACCOUNT_IDENTIFIER }}/{{ SNOWFLAKE_DB }}/{{ SNOWFLAKE_SCHEMA }}?warehouse={{SNOWFLAKE_WAREHOUSE }}"
  test_sqlite:
    db.url: "sqlite:///tmp/test_sqlite.db"
  test_postgres:
    db.url: "postgresql://demouser:demopass@localhost/covid_cz"
  test_mysql:
    db.url: "mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4&local_infile=1"
  test_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
storages:
  s3_covid_cz:
    url: "{{ AWS_COVID_STAGE_S3_URL }}"
    access_key: "{{ AWS_COVID_STAGE_S3_ACCESS_KEY }}"
    secret_key: "{{ AWS_COVID_STAGE_S3_S3_SECRET_KEY }}"

