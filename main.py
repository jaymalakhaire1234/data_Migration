import sqlalchemy as sal
import pymysql
from pymysql import connect
import pyodbc
from sqlalchemy.orm import query_expression

import pandas as pd
import json
import smtplib
import smtplib
import logging
from utils.config_file_read import read_mssql,read_mysql
from utils.config_file_read import read_email
from utils.db_connection import db_mssql_conn,db_mysql_conn
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
logging.basicConfig(filename='server.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')



def data_magretion():
    try:
        mysql= db_mysql_conn()    
        ssms = db_mssql_conn()
        query = ("SELECT * FROM ibm_employee")
        logging.info("Read Query from Mysql")
        df = pd.read_sql(query, mysql)
        df.to_sql("tcs1",ssms,if_exists="replace",index=True)
        logging.info("Data Migrate succesfully from Mysql to Mssql")
        mysql.close()
        ssms.close()
        logging.info("Closed connection")
        return True
    except Exception as e:
        print(e)
        return False
# Send email
def send_gmail(status):
          
    gmail_cread = read_email()
    sender_mail = "jaymalakhaire9766@gmail.com"
    reciver_mail = "khairejaymala@gmail.com"
    password = f"{gmail_cread['apppass']}"
    subject = "Migrate the data from mysql to mssql"
    
    mysql_conn = 'ibm_employee'
    ssms_conn = 'tcs'
    
    if status:
        body = f"Hi team,\n\n{mysql_conn} The data is successfully loaded  {ssms_conn} in SQL Server DB.\n\nBest regards,\nJaymala Khaire."
    else:
        body = f"Hi team,\n\n Sorry !The data migration from MySQL DB {mysql_conn} to SQL Server DB {ssms_conn} is failed. Please check the logs and your code\n\nBest regards,\nJaymala Khaire."

    msg = MIMEMultipart()
    msg['From'] = sender_mail
    msg['To'] = reciver_mail
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to SMTP server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_mail, password)
            server.sendmail(sender_mail, reciver_mail, msg.as_string())
            print("Email sent successfully.")
    except Exception as e:
        print(e)



def main():
    status = data_magretion()
    send_gmail(status)
    
if __name__ == "__main__":
    main()





