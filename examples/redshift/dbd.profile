databases:
  basic_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
  covid_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
  covid_cz_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
storages:
  s3_covid_cz:
    url: "{{ AWS_COVID_STAGE_S3_URL }}"
    access_key: "{{ AWS_COVID_STAGE_S3_ACCESS_KEY }}"
    secret_key: "{{ AWS_COVID_STAGE_S3_S3_SECRET_KEY }}"
