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
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
logging.basicConfig(filename='server.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')

def read_config():
    try:
        with open('Data_Migration\config\mysql.json') as fp:
            cred = json.load(fp)
        logging.info("Sucessfully read mysql.config")

        mysql_engine = sal.create_engine(f"mysql+pymysql://{cred['Username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['Database']}")
        mysql_conn = mysql_engine.connect()
        
        
        logging.info("Successfully connect to mysql Database")

        with open('Data_Migration\config\ssms.json') as s_fp:
            sql_cred = json.load(s_fp)
        logging.info("Sucessfully read mssql.config")

        ssms_engine = sal.create_engine(f"mssql+pyodbc://{sql_cred['host']}/{sql_cred['database']}?driver=ODBC+Driver+17+for+SQL+Server")
        ssms_conn = ssms_engine.connect()
        logging.info("Successfully connect to Myssms Database")


        

        query = ("SELECT * FROM sbi_bank_customers1")
        logging.info("Read Query from Mysql")
        df = pd.read_sql(query, mysql_conn)
        df.to_sql("tcs",ssms_conn,if_exists="replace",index=True)
        logging.info("Data Migrate succesfully from Mysql to Mssql")
        mysql_conn.close()
        ssms_conn.close()
        logging.info("Closed connection")
        return True
    except Exception as e:
        print(e)
        return False
# Send email
def send_email(status):
    with open('Data_Migration\config\gmail.json') as fp:
        gmail = json.load(fp)
    sender_mail = "jaymalakhaire9766@gmail.com"
    reciver_mail = "khairejaymala@gmail.com"
    password = f"{gmail['apppass']}"
    subject = "Migrate the data from mysql to mssql"
    
    mysql_conn = 'sbi_bank_customers1'
    ssms_conn = 'tcs'
    
    if status:
        body = f"Hi team,\n\n{mysql_conn} The data is successfully loaded  {ssms_conn} in SQL Server DB.\n\nBest regards,\nJaymala Khaire."
    else:
        body = f"Hi team,\n\n Sorry !The data migration from MySQL DB {mysql_conn} to SQL Server DB {ssms_conn} is failed. Please check the logs and your code\n\nBest regards,\nJaymala Khaire."

    msg = MIMEMultipart()
    msg['From'] = sender_mail
    msg['To'] = reciver_mail
    msg['Subject'] = subject
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
        print(f"Failed to send email: {e}")



def main():
    status = read_config()
    send_email(status)
if __name__ == "__main__":
    main()





