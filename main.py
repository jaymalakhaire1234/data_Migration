import sqlalchemy as sal
import pymysql
from pymysql import connect
import pyodbc
from sqlalchemy.orm import query_expression

import pandas as pd
import json
import smtplib
import smtplib


def read_config():
    try:
        with open('config\mysql.json') as fp:
            cred = json.load(fp)

        mysql_engine = sal.create_engine(f"mysql+pymysql://{cred['Username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['Database']}")
        mysql_conn = mysql_engine.connect()

        with open('config\ssms.json') as s_fp:
            sql_cred = json.load(s_fp)

        ssms_engine = sal.create_engine(f"mssql+pyodbc://{sql_cred['host']}/{sql_cred['database']}?driver=ODBC+Driver+17+for+SQL+Server")
        ssms_conn = ssms_engine.connect()


        

        query = ("SELECT * FROM sbi_bank_customers")
        df = pd.read_sql(query, mysql_conn)
        df.to_sql("tcs",ssms_conn,if_exists="fail",index=True)
        print("Data Fetch succesfully")
        mysql_conn.close()
        ssms_conn.close()
        
    except Exception as e:
        print(e)
        

def main():
    read_config()
if __name__ == "__main__":
    main()





