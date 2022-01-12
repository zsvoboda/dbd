databases:
  basic_mysql:
    db.url: "mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4"
  covid_mysql:
    db.url: "mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4"
  covid_cz_mysql:
    db.url: "mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4"
