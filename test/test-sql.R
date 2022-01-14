library(EQ.SQL)

eq.init()

SQL.insert("emews_points", list("eq_id"), list(12))
SQL.update("emews_points", list("status"), list(1), "eq_id=12")
