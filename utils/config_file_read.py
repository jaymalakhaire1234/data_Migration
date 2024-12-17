import pymysql
import pyodbc
import sqlalchemy as sal
import json

def read_mysql():
    try:
        with open('config\mysql.json') as fp:
            cred = json.load(fp)
        return cred
    except Exception as e:
        print(e)

def read_mssql():
    try:
        with open('config\ssms.json') as s_fp:
            sql_cred = json.load(s_fp)
        return sql_cred
    except Exception as e:
        print(e)

def read_email():
    try:
        with open('config\gmail.json') as fp:
            gmail = json.load(fp)
        return gmail
    except Exception as e:
        print(e)

    
        
