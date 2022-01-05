databases:
  states_snowflake:
    db.url: "snowflake://{{ SNOWFLAKE_USER }}:{{ SNOWFLAKE_PASSWORD }}@{{ SNOWFLAKE_ACCOUNT_IDENTIFIER }}/{{ SNOWFLAKE_DB }}/{{ SNOWFLAKE_SCHEMA }}?warehouse={{SNOWFLAKE_WAREHOUSE }}"
