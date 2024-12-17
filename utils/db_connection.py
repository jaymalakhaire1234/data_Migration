import pymysql
import sqlalchemy as sal
import pyodbc
import pandas as pd
from utils.config_file_read import read_mysql,read_mssql

def db_mysql_conn():
    try:
        cred = read_mysql()
        mysql_engine = sal.create_engine(f"mysql+pymysql://{cred['Username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['Database']}")
        mysql_conn = mysql_engine.connect()
        return mysql_conn
    except Exception as e:
        print(e)
        
def db_mssql_conn():
    try:
        sql_cred = read_mssql()
        ssms_engine = sal.create_engine(f"mssql+pyodbc://{sql_cred['host']}/{sql_cred['database']}?driver=ODBC+Driver+17+for+SQL+Server")
        ssms_conn = ssms_engine.connect()
        
    except Exception as e:
        print(e)
    
