import json
import pymysql
import pyodbc
import pandas as pd
import sqlalchemy as sal

# with open ("config\\mysql.json") as myfp:
#     cred = json.load(myfp)
# mysqlconn = sal.create_engine(f"mysql+pymysql://{cred['Username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['Database']}")
# mysql = mysqlconn.connect()

# q = ("select * from sbi_bank_customers")
# df = pd.read_sql(q,mysql)
# print(df)

with open ("config\ssms.json") as msql:
    data = json.load(msql)
sql_server_engine = sal.create_engine(f"mssql+pyodbc://{data['host']}/{data['database']}?driver=ODBC+Driver+17+for+SQL+Server")
conn = sql_server_engine.connect()

q= ("select * from emp")
df = pd.read_sql(q,conn)
print(df)