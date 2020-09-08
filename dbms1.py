import pandas as pd
import pyodbc
import random
connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-4ONR3F7\MSSQLSERVER01;'
                      'Database=DBMS;'
                      'Trusted_Connection=yes;')
mycursor = connection.cursor()
def maketables():
    df=pd.read_csv('naukri_com-job_sample.csv')
    df1=pd.read_csv('candidate.csv')
    kf=pd.read_csv('user-languages.csv')
    qdf=pd.read_csv('questions.csv')
    skill_name=[]
    ctr=0
    a=list(df['jobtitle'])
    b=list(df['joblocation_address'])
    c=list(df['jobid'])
    d=list(df['jobdescription'])
    candidate_name=list(df1['Employee_Name'])
    candidate_id=list(df1['EmpID'])
    candidateage=[i for i in range(20,30)]
    employeeage=[i for i in range(25,65)]
    question_id=qdf['QCode']
    question_description=qdf['link']
    question_difficulty=qdf['level']
    question_tags=qdf['Tags']
    question_editorial=qdf['Editorial']
    for i in range(1,len(kf.columns)):
        if kf.columns[i] not in skill_name:
            ctr+=1
            skill_name.append(kf.columns[i])
    education=list(df['education'])
    experience=list(df['experience'])
    t0=[]
    t1=[]
    t2=[]
    t3=[]
    t4=[]
    t5=[]
    t6=[]
    t7=[]
    t8=[]
    t9=[]
    t10=[]
    sql11="insert into QUESTION_TAGS(QUESTION_ID,TAGS) values (?,?)"
    sql10="insert into INTERVIEWER(EMPLOYEE_ID) values (?)"
    sql9="insert into QUESTION_DIFFICULTY(QUESTION_ID,DIFFICULTY) values (?,?)"
    sql8="insert into QUESTION_EXPLANATION(QUESTION_ID,EXPLANATION) values (?,?)"
    sql7="insert into QUESTION(QUESTION_DESCRIPTION) values (?)"
    sql6="insert into EMPLOYEE(EMPLOYEE_NAME,EMPLOYEE_AGE,EMPLOYEE_POSITION) values (?,?,?)"
    sql5="insert into SKILLS(CANDIDATE_ID,SKILL_ID,LEVEL) values (?,?,?)"
    sql3="insert into CANDIDATE(EXPERIENCE,NAME,AGE,EDUCATION,ROLE) values (?,?,?,?,?)"
    sql1="insert into JOB_DETAILS(LOCATION,JOB_TITLE,FILLED) values (?,?,?)"
    sql2="insert into JOB_ROLE(JOB_TITLE,JOB_DESCRIPTION) values (?,?)"
    sql4="insert into SKILL_DETAILS(SKILL_NAME) values (?)"
    interviewer_id=[]
    for i in range(1,76):
        for location in b[i].split(','):
            t0.append((location,a[i],'False'))
        t1.append((a[i],d[i][41:]))
        if str(education[i])=='nan':
            ede=None
        else:
            ede=str(education[i])
        t2.append((str(experience[i]),str(candidate_name[i]),random.choice(candidateage),ede,random.randint(76,123)))
        t5.append((str(candidate_name[i+76]),random.choice(employeeage),i))
        if len(interviewer_id)<37:
            while 1:
               curinterviewer_id=random.randint(1,75)
               if curinterviewer_id not in interviewer_id:
                   break     
            t9.append((curinterviewer_id,))
            interviewer_id.append(curinterviewer_id)
            
    for i in range(1,ctr):
        t3.append((skill_name[i],))
    for i in range(1,76):
        w=[]
        while len(w)<3:
            a=random.randint(1,1409)
            if a not in w:
                w.append(a)
        for j in range(3):
            t4.append((i,w[j],random.randint(0,10)))
    for i in range(1,len(question_id)+1):
        str1=""
        for j in question_tags[i-1]:
            if j=='[' or j==']':
                pass
            elif j!=',':
               str1+=j
            else:
               str1+=" "
        str1=str1.split()
        t6.append((str(question_description[i-1]),))
        if str(question_editorial[i-1])=='nan':
            ed=None
        else:
            ed=str(question_editorial[i-1])
        t7.append((i,ed))
        t8.append((i,str(question_difficulty[i-1])))
        for k in str1:
            t10.append((i,k))
    for job in range(75):
        t0[job]=(t0[job][0],t0[job][1],"True")
    mycursor.executemany(sql1,t0)
    mycursor.executemany(sql2,t1)
    mycursor.executemany(sql3,t2)
    mycursor.executemany(sql4,t3)
    mycursor.executemany(sql5,t4)
    mycursor.executemany(sql6,t5)
    mycursor.executemany(sql7,t6)
    mycursor.executemany(sql8,t7)
    mycursor.executemany(sql9,t8)
    mycursor.executemany(sql10,t9)
    mycursor.executemany(sql11,t10)
    connection.commit()

def makeinterviewtable():
    result=['SOLVED','UNSOLVED','PARTIALLY SOLVED']
    interviewerID=list(mycursor.execute('select INTERVIEWER_ID from INTERVIEWER'))
    candidates=list(mycursor.execute('select CANDIDATE_ID,STATUS from CANDIDATE'))
    q=list(mycursor.execute('select QUESTION_ID from QUESTION'))
    for i in range(len(candidates)):
        if candidates[i][1]=='entry recieved':
            curinterviewer=random.choice(interviewerID)
            candidate_ID=candidates[i][0]
            res=random.choice(result)
            if res=='SOLVED':
                score=10
            elif res=='UNSOLVED':
                score=0
            else:
                score=None
            question_current=random.choice(q)
            sql1="insert into INTERVIEW(CANDIDATE_ID,INTERVIEWER_ID,RESULT,SCORE) output Inserted.INTERVIEW_ID values (?,?,?,?)"
            mycursor.execute('update CANDIDATE SET STATUS=\'ongoing\' WHERE CANDIDATE_ID=?',candidate_ID)
            interviewid=mycursor.execute(sql1,(candidate_ID,curinterviewer[0],res,score)).fetchone()[0]
            sql2="insert into MAP(INTERVIEW_ID,QUESTION_ID) values (?,?)"
            mycursor.execute(sql2,(interviewid,question_current[0]))
    connection.commit()
    
            
            
          
            
maketables()
makeinterviewtable()



