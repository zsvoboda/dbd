databases:
  basic:
    db.url: "mysql+pymysql://{{ MYSQL_USER }}:{{ MYSQL_PASSWORD }}@{{ MYSQL_HOST }}/{{ MYSQL_DB }}?charset=utf8mb4"