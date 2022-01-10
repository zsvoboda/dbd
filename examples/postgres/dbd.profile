databases:
  covid_cz_pgsql:
    db.url: "postgresql://{{ POSTGRES_USER }}:{{ POSTGRES_PASSWORD }}@{{ POSTGRES_HOST }}/{{ POSTGRES_DB }}"
  covid_pgsql:
    db.url: "postgresql://{{ POSTGRES_USER }}:{{ POSTGRES_PASSWORD }}@{{ POSTGRES_HOST }}/{{ POSTGRES_DB }}"
