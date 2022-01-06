databases:
  covid_cz:
    db.url: "postgresql://{{ POSTGRES_USER }}:{{ POSTGRES_PASSWORD }}@{{ POSTGRES_HOST }}/{{ POSTGRES_DB }}"
