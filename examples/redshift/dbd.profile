databases:
  basic_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
  covid_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
  covid_cz_redshift:
    db.url: "redshift+psycopg2://{{ REDSHIFT_USER }}:{{ REDSHIFT_PASSWORD }}@{{ REDSHIFT_HOST }}:{{ REDSHIFT_PORT }}/{{ REDSHIFT_DB }}"
storage:
  s3_covid_cz:
    url: "s3://{{ S3_BUCKET }}/{{ S3_PATH }}"
    access_key: "{{ AWS_S3_ACCESS_KEY }}"
    secret_key: "{{ AWS_S3_SECRET_KEY }}"
