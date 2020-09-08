import pyodbc
import random
connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-4ONR3F7\MSSQLSERVER01;'
                      'Database=DBMS;'
                      'Trusted_Connection=yes;')
mycursor = connection.cursor()
def get_job_using_id(job_id):
    sql='select*from JOB_DETAILS,JOB_ROLE WHERE JOB_ID=?'
    a=mycursor.execute(sql,job_id).fetchall()
    return a
def get_available_jobs_at_location(location):
    sql='select*from JOB_DETAILS,JOB_ROLE WHERE LOCATION=?'
    a=mycursor.execute(sql,location).fetchall()
    return a

job_id=int(input())
res1=get_job_using_id(job_id)
print(res1)
print("**************************")
