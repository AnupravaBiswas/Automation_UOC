import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
##from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import time
from time import sleep
import schedule
import os
import re
import numpy as np
from pandas import ExcelWriter
import xlsxwriter
import runpy
import smtplib
import csv
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import datetime
from email import encoders
import imaplib

pd.set_option("display.max_columns", 100)


#credentials
username = "snoc.hydinfra"
f = open("password.txt", "r")
passwrd = f.read()
password = str(passwrd)

# initialize the Chrome driver
driver = webdriver.Chrome("chromedriver")


# head to login page
driver.get("http://10.19.33.84:3000/login")

sleep(80)
# find username/email field and send the username itself to the input field
driver.find_element_by_id("username").send_keys(username)
# find password input field and insert password as well
driver.find_element_by_id("password").send_keys(password)
# click login button
driver.find_element_by_name("login").click()

sleep(8)

driver.get('http://10.19.33.84:3000/workspaces/bss-infra_hyd_raw_ws')

sleep(310)



def getting_data():
    
    
    ##getting data fields
    lst =[]
    results = driver.find_elements_by_id('alarmTableRow')
    for data in results:
        dataArr = data.text
        print(dataArr)
        lst.append(dataArr)
    
    
    #segreate data into rows
    composite_list = [lst[x:x+16] for x in range(0, len(lst),16)]

    print (composite_list)
    
    #creating dataframe
    df_x = pd.DataFrame(composite_list, columns=['Node_Type', 'Circle', 'Node_Name', 'Uoc_Timestamp', 'Specific_ProblemID', 'ALSpecific_Problem','Sub_Specific_Problem','OPERATOR','VENDOR','Technology','Node_ID','IS_Hub','ZONE','Node_Status','Engineer_Name','Engineer_Mobile'])
    df_x
    
    #saving data in csv
     
    if not os.path.isfile('alarm.csv'):

       df_x.to_csv('alarm.csv', header='column_names',index=False)

    else: # else it exists so append without mentioning the header

       df_x.to_csv('alarm.csv', mode='a+', header=False, index=False)


    # reading the alarm file and manipulating to combine datas of same Node name
    df = pd.read_csv('alarm.csv')
    df2 = df
    #df2 = df[~df['Specific ProblemID'].isin(['RECTIFIER_MINOR','RECTIFIER_MAJOR','RECTIFIER_MAJOR','198087303', '7402', '7406', '198087304', '198087314', '7405', '7411', '199087790', '7403', '7401', '4001', '198087176', '198087175', '7412','7404', '7103', '7606', '7115','199066019', '7602','7653', '7107', '7767','7409', '198087291', 'RECTIFIER_MAJOR', '7407','RECTIFIER_MINOR', '198087173', '198087005', '198087315', '198087003','198087017', '7408', '7410','198092572', '199087797', '199087789', '198087006','198087170', '199087778', '198087007', '198087174', '198087316', '198087306', '199087794', '199087791', '199083864', '198087313', '198087314','198087306','198087314','198087304','198087314','198087291','198087316','198087306','198087291'])]


    df2 = df2.replace(r'^\s*$', 'No Data', regex=True)
    df2 = df2.replace(np.nan, 'No Data')

    df3 = df2.sort_values(['Node_Name', 'Node_ID'])
    df3.reset_index(drop=True, inplace=True)


    # Create the list where we 'll capture the cells that appear for 1st time,
    # add the 1st row and we start checking from 2nd row until end of df
    startCells = [1]
    for row in range(2,len(df3)+1):
        if (df3.loc[row-1,'Node_Name'] != df3.loc[row-2,'Node_Name']):
            startCells.append(row)


    writer = pd.ExcelWriter('test.xlsx', engine='xlsxwriter')
    df3.to_excel(writer, sheet_name='Sheet1', index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    merge_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 2})


    lastRow = len(df3)

    for row in startCells:
        try:
            endRow = startCells[startCells.index(row)+1]-1
            if row == endRow:
                worksheet.write(row, 0, df3.loc[row-1,'Node_Name'], merge_format)
            else:
                worksheet.merge_range(row, 0, endRow, 0, df3.loc[row-1,'Node_Name'], merge_format)
        except IndexError:
            if row == lastRow:
                worksheet.write(row, 0, df3.loc[row-1,'Node_Name'], merge_format)
            else:
                worksheet.merge_range(row, 0, lastRow, 0, df3.loc[row-1,'Node_Name'], merge_format)


    writer.save()

###-------------------JOING THE DATA--------------------------------##
f2  = pd.read_excel("details.xlsx")
f2 = f2.drop(columns = ['Site Name', 'NSS ID', 'IP NAME', 'IP ID', 'Zone', 'Site Engineer Name','Site Engineer Contact', 'Site Engineer Email', 'Cluster_Manager Name','Cluster_Manager_Contact', 'Cluster_Manager_Email','Zonal Incharge Name', 'Zonal Incharge Contact','Zonal Incharge_E mail', 'O&M Head Name', 'O&M Head Name No.','O&M Head _E mail', 'IP Provider (CM Name)', 'IP Provider (CM Contact)','IP Provider (CM Mail ID)', 'IP Provider (O&M Head Name)','IP Provider (O&M Head Contact)', 'IP Provider (O&M Head E-mail)','SNOC Infra Manager', 'SNOC Shift LEAD', 'SNOC BSS DESK','Circle', 'Site Type(BSC/RNC/Hub/Normal Site )'])
f2 = f2.drop_duplicates(subset='Node_Name', keep="first")


def convert_tst_data():
    global f1
    f1  = pd.read_excel("test.xlsx")    
    f1_new = f1.groupby(['Node_Name','Circle','OPERATOR','VENDOR','Technology','Node_ID','IS_Hub','ZONE','Node_Status','Engineer_Name'], as_index=False).agg(lambda col: ','.join(col))
    f1_new.replace(np.nan, '', regex=True)
    f3 = pd.merge(f1_new, f2,on='Node_Name',  how='left')
    f3 = f3.drop(columns = ['Engineer_Mobile'])
    f3.to_csv('mail.csv',index = False)





###---------------------MAIL SCRIPT-----------------------------------##
##import win32com.client as client
##outlook = client.Dispatch("Outlook.Application")

CREDENTIALS_USER ="bab534699@gmail.com"
###read from password.txt
##f = open("password.txt", "r")
##passwrd = f.read()
CREDENTIALS_PASS = ''#str(passwrd)
EMAIL_FROM_DEFAULT = "bab534699@gmail.com"

##EMAIL_SUBJECT = "Hello There!"
##EMAIL_CC_DEFAULT = ""
##EMAIL_BCC_DEFAULT = ""

# Must be a csv file containing  
CONTACTS_FILE = "mail.csv"

def mailsendingoutlook():
    
    # Set your Email Template here
    def getEmailContent(first_name, Node_Name, Circle, OPERATOR, VENDOR, Technology, Node_ID, IS_Hub,ZONE, Node_Status, Uoc_Timestamp, ALSpecific_Problem, Sub_Specific_Problem):

        email_content ="""
    Hi Team,
    <br>
    BSS INFRA ALARM NOTIFICATION!
    <br>
    As informed to MR """+first_name+""", Request your kind intervention & support to clear below appended alarms.
    <br><br>"""+Node_Name+"||"+Circle+"|| "+OPERATOR+" ||"+VENDOR+"|| "+Technology+"|| "+Node_ID+" ||"+IS_Hub+ZONE+"|| "+Node_Status+" ||"+Uoc_Timestamp+"|| "+ALSpecific_Problem+"|| "+Sub_Specific_Problem+"""<sbr><br>
    Regards,<br>
    Anuprava<br>
    From SNOC Infra desk!<br>
    Ph No :7064267679<br>
        """

        return email_content

    # Loops your contacts list (csv file)
    def loop_contacts(filename):
        print('looping contacts')

        # Set up the SMTP server
        print('Sending emails')
        s = smtplib.SMTP(host='smtp.gmail.com', port=587)
        s.starttls()
        s.login(CREDENTIALS_USER, CREDENTIALS_PASS)


        count = 1

        with open(filename, mode='r') as contacts_file:
            reader = csv.reader(contacts_file)
            next(reader)
            for contact in reader:
                #contact_full_name = contact[9]
                Node_Name = contact[0]
                Circle = contact[1]
                OPERATOR = contact[2]
                VENDOR = contact[3]
                Technology = contact[4]
                Node_ID = contact[5]
                IS_Hub = contact[6]
                ZONE = contact[7]
                Node_Status  = contact[8]
                Uoc_Timestamp = contact[10]
                ALSpecific_Problem = contact[11]
                Sub_Specific_Problem = contact[12]
                #title = contact[2]
                Tomail = contact[14]
                CC_MAIL = contact[15]
                first_name= contact[9]

                msg = MIMEMultipart()

                print(count)
                count = count +1
                print("Sending email to", first_name)

                msg['From']=EMAIL_FROM_DEFAULT
                msg['To']=Tomail
                msg['cc'] = CC_MAIL
                msg['Subject']=  f'INFRA ALARM NOTIFICATION || BSC/RNC/HUB NAME:-{Node_Name}|| BSC/RNC/HUB ALARM Name:-{ALSpecific_Problem}{Sub_Specific_Problem}'

                try:
                    
                    msg.attach(MIMEText(getEmailContent(first_name, Node_Name, Circle, OPERATOR, VENDOR, Technology, Node_ID, IS_Hub,ZONE, Node_Status, Uoc_Timestamp, ALSpecific_Problem, Sub_Specific_Problem), 'html'))
                    print(type(msg))
                    s.send_message(msg)
                except (smtplib.SMTPException, smtplib.SMTPServerDisconnected, smtplib.SMTPResponseException, smtplib.SMTPSenderRefused, smtplib.SMTPRecipientsRefused, smtplib.SMTPDataError, smtplib.SMTPConnectError, smtplib.SMTPHeloError):
                    pass

                del msg

        s.quit()

    loop_contacts(CONTACTS_FILE)

    


schedule.every(3).minutes.do(getting_data)
schedule.every(5).minutes.do(convert_tst_data)
schedule.every(7).minutes.do(mailsendingoutlook)
while 1:
    schedule.run_pending()
    time.sleep(1)



        





