import json
import pandas as pd
import os
import glob
import schedule
import time
import xlrd
import pyodbc
from datetime import datetime
import smtplib



conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=192.168.0.117;'
                      'Database=BI;'
                      'UID=BI;'
                      'PWD=BI$%^app;'
                      'Trusted_Connection=no;'
                      )

# files_path = os.path.join('E:\AJWORLD\FTP Automation\DATA\*')
files_path = os.path.join('C:\CW1_Reports\*')
files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True)
today_date = datetime.today().strftime('%Y-%m-%d')


def data_fil(row):
    if row['Job Dept'] == "FEA":
        val = "AIR EXPORT"
        
    elif row['Job Dept'] == "FIA":
        val = "AIR IMPORT"
        
    elif row['Job Dept'] == "FDR":
        val = "TRANSPORT"
        
    elif row['Job Dept'] == "FES" and row['Cont'] == "LCL":
        val = "LCL EXPORT"        
        
    elif row['Job Dept'] == "FIS" and row['Cont'] == "LCL":
        val = "LCL IMPORT"        
        
    elif row['Job Dept'] == "FES" and row['Cont'] != "LCL":
        val = "FCL EXPORT"        
        
    elif row['Job Dept'] == "FIS" and row['Cont'] != "LCL":
        val = "FCL IMPORT"
        
    elif row['Job Dept'] == "WFS":
        val = "WAREHOUSE"
        
    elif row['Job Dept'] == "WFW":
        val = "WAREHOUSE"
        
    elif row['Job Dept'] == "MIS":
        val = "SERVICE JOB"
    else:
        val = "Other"
    return val



print("Revenue Worked")
file_name = ""
for i in files:
    if ".CSV" in i:
        if "NYC Job Profit" in i:
            if today_date in i:
                try:
                    file_name = i
                    datem = datetime.today().strftime("%Y-%m")
                    month = datetime.today().strftime("%Y-%m")
                    df = pd.read_csv(r""+str(file_name)+"")
                    df = df.dropna(how='all')
                    df['Segments'] = df.apply(data_fil, axis=1)
                    df_date_check = pd.read_sql("select * from revenue_data_important where CONVERT(VARCHAR, Transaction_Date)='"+month+"'",conn)
                    print(df_date_check)
                    df_filterd = df[["Job #","Job Local Ref","Job Brn.","Job Dept","Cont","Segments","Job Stat","Rep","SalesRep Name","Ops","Ops Name","Controlling Customer","Controlling Customer Roll","Job Rev Recognition","Consignee Full Name","House Bill Number","Job Local Client AR Settlement Group Full Name","Job Overseas Agent AR Settlement Group Full Name","Job Local Client AR Settlement Group Code","Consignor/Shipper/Supplier Full Name","Total Income","Cost","Accrual","Total Expense","Job Profit", "Rev Recognition Month"]]
                    df_filterd = df[["Job #","Job Local Ref","Job Brn.","Job Dept","Cont","Segments","Job Stat","Rep","SalesRep Name","Ops","Ops Name","Controlling Customer","Controlling Customer Roll","Job Rev Recognition","Consignee Full Name","House Bill Number","Job Local Client AR Settlement Group Full Name","Job Overseas Agent AR Settlement Group Full Name","Job Local Client AR Settlement Group Code","Consignor/Shipper/Supplier Full Name","Total Income","Cost","Accrual","Total Expense","Job Profit", "Rev Recognition Month"]]
                    df_filterd["Job Rev Recognition Before Comma"] = df_filterd["Job Rev Recognition"].str.split(',').str[0]
                    df_filterd["Job Rev Recognition After Comma"] = df_filterd["Job Rev Recognition"].str.split(',').str[1]
                    df_filterd["Job Rev Recognition Date"] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[1]
                    df_filterd["Job Rev Recognition Dpt"] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[0]
                    df_filterd = df_filterd.astype(str)
                    df_filterd["Job Rev Recognition Date After Comma"] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[0]
                    df_filterd["Job Rev Recognition Dpt After Comma"] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[1]
                    df_filterd["Job Local Ref"] = df_filterd["Job Local Ref"].str.replace(',','').astype(float)
                    df_filterd["Total Income"] = df_filterd["Total Income"].str.replace(',','').astype(float)
                    df_filterd["Cost"] = df_filterd["Cost"].str.replace(',','').astype(float)
                    df_filterd["Accrual"] = df_filterd["Accrual"].str.replace(',','').astype(float)
                    df_filterd["Total Expense"] = df_filterd["Total Expense"].str.replace(',','').astype(float)
                    df_filterd["Job Profit"] = df_filterd["Job Profit"].str.replace(',','').astype(float)
                    df_filterd["Job Rev Recognition Date After Comma"] = df_filterd["Job Rev Recognition Date After Comma"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = df_filterd["Job Rev Recognition Date"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = pd.to_datetime(df_filterd["Job Rev Recognition Date"])
                    df_filterd["Job Rev Recognition Date After Comma"] = pd.to_datetime(df_filterd["Job Rev Recognition Date After Comma"])
                    df_filterd["Job Rev Recognition Date After Comma"] = df_filterd["Job Rev Recognition Date After Comma"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = df_filterd["Job Rev Recognition Date"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = pd.to_datetime(df_filterd["Job Rev Recognition Date"])
                    df_filterd["Job Rev Recognition Date After Comma"] = pd.to_datetime(df_filterd["Job Rev Recognition Date After Comma"])
                    df_filterd["Rev Recognition Month"] = pd.to_datetime(df_filterd["Rev Recognition Month"], format="%b-%y")
                    df_filterd["Transaction_Date"] = month
                    df_filterd = df_filterd.fillna("")

                    if month == datem:
                        if len(df_date_check) > 0:
                            sql_del = "DELETE FROM revenue_data_important where CONVERT(VARCHAR, Transaction_Date)='"+month+"'"
                            cursor = conn.cursor()
                            cursor.execute(sql_del)
                            conn.commit()
                            for index,row in df_filterd.iterrows():
                                sql = "insert into revenue_data_important( [Job #] , [Job Local Ref] , [Job Brn] , [Job Dept] , [Cont] , [Segments] , [Job Stat] , [Rep] , [SalesRep Name] , [Ops] , [Ops Name] , [Controlling Customer] , [Controlling Customer Roll] ,[Job Rev Recognition] ,[Consignee Full Name] , [House Bill Number] , [Job Local Client AR Settlement Group Full Name] , [Job Overseas Agent AR Settlement Group Full Name] , [Consignor Shipper Supplier Full Name] ,[Total Income], [Cost], [Accrual], [Total Expense], [Job Profit], [Job Rev Recognition Date], [Job Rev Recognition Dpt] , [Job Rev Recognition Date After Comma] , [Job Rev Recognition Dpt After Comma] , [Transaction_Date], [Delete_status], [Rev Recognition Month]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                params = (row["Job #"] , row["Job Local Ref"] , row["Job Brn."] , row["Job Dept"] , row["Cont"] , row["Segments"] , row["Job Stat"] , row["Rep"] , row["SalesRep Name"] , row["Ops"] , row["Ops Name"] , row["Controlling Customer"] , row["Controlling Customer Roll"] ,row["Job Rev Recognition"] ,row["Consignee Full Name"] , row["House Bill Number"] ,  row["Job Local Client AR Settlement Group Full Name"] , row["Job Overseas Agent AR Settlement Group Full Name"] ,row["Consignor/Shipper/Supplier Full Name"] ,row["Total Income"], row["Cost"], row["Accrual"], row["Total Expense"], row["Job Profit"], row["Job Rev Recognition Date"], row["Job Rev Recognition Dpt"] , row["Job Rev Recognition Date After Comma"] , row["Job Rev Recognition Dpt After Comma"] , row["Transaction_Date"],'No',row["Rev Recognition Month"])
                                cursor = conn.cursor()
                                cursor.execute(sql,params)
                                print(index , " - Data Deleted and Row Inserted")
                                conn.commit()
                        else:
                            for index,row in df_filterd.iterrows():
                                sql = "insert into revenue_data_important( [Job #] , [Job Local Ref] , [Job Brn] , [Job Dept] , [Cont] , [Segments] , [Job Stat] , [Rep] , [SalesRep Name] , [Ops] , [Ops Name] , [Controlling Customer] , [Controlling Customer Roll] ,[Job Rev Recognition] ,[Consignee Full Name] , [House Bill Number] , [Job Local Client AR Settlement Group Full Name] , [Job Overseas Agent AR Settlement Group Full Name] , [Consignor Shipper Supplier Full Name] ,[Total Income], [Cost], [Accrual], [Total Expense], [Job Profit], [Job Rev Recognition Date], [Job Rev Recognition Dpt] , [Job Rev Recognition Date After Comma] , [Job Rev Recognition Dpt After Comma] , [Transaction_Date], [Delete_status], [Rev Recognition Month]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                params = (row["Job #"] , row["Job Local Ref"] , row["Job Brn."] , row["Job Dept"] , row["Cont"] , row["Segments"] , row["Job Stat"] , row["Rep"] , row["SalesRep Name"] , row["Ops"] , row["Ops Name"] , row["Controlling Customer"] , row["Controlling Customer Roll"] ,row["Job Rev Recognition"] ,row["Consignee Full Name"] , row["House Bill Number"] ,  row["Job Local Client AR Settlement Group Full Name"] , row["Job Overseas Agent AR Settlement Group Full Name"] ,row["Consignor/Shipper/Supplier Full Name"] ,row["Total Income"], row["Cost"], row["Accrual"], row["Total Expense"], row["Job Profit"], row["Job Rev Recognition Date"], row["Job Rev Recognition Dpt"] , row["Job Rev Recognition Date After Comma"] , row["Job Rev Recognition Dpt After Comma"] , row["Transaction_Date"],'No', row["Rev Recognition Month"])
                                cursor = conn.cursor()
                                cursor.execute(sql,params)
                                print(index , " -Data Not Deleted and  Row Inserted")
                                conn.commit()
                    else:
                        sql_del = "Update revenue_data_important SET Delete_status='Yes' where CONVERT(VARCHAR, Transaction_Date)='"+month+"'"
                        cursor = conn.cursor()
                        cursor.execute(sql_del)
                        conn.commit()
                        for index,row in df_filterd.iterrows():
                                sql = "insert into revenue_data_important( [Job #] , [Job Local Ref] , [Job Brn] , [Job Dept] , [Cont] , [Segments] , [Job Stat] , [Rep] , [SalesRep Name] , [Ops] , [Ops Name] , [Controlling Customer] , [Controlling Customer Roll] ,[Job Rev Recognition] ,[Consignee Full Name] , [House Bill Number] , [Job Local Client AR Settlement Group Full Name] , [Job Overseas Agent AR Settlement Group Full Name] , [Consignor Shipper Supplier Full Name] ,[Total Income], [Cost], [Accrual], [Total Expense], [Job Profit], [Job Rev Recognition Date], [Job Rev Recognition Dpt] , [Job Rev Recognition Date After Comma] , [Job Rev Recognition Dpt After Comma] , [Transaction_Date], [Delete_status], [Rev Recognition Month]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                params = (row["Job #"] , row["Job Local Ref"] , row["Job Brn."] , row["Job Dept"] , row["Cont"] , row["Segments"] , row["Job Stat"] , row["Rep"] , row["SalesRep Name"] , row["Ops"] , row["Ops Name"] , row["Controlling Customer"] , row["Controlling Customer Roll"] ,row["Job Rev Recognition"] ,row["Consignee Full Name"] , row["House Bill Number"] ,  row["Job Local Client AR Settlement Group Full Name"] , row["Job Overseas Agent AR Settlement Group Full Name"] ,row["Consignor/Shipper/Supplier Full Name"] ,row["Total Income"], row["Cost"], row["Accrual"], row["Total Expense"], row["Job Profit"], row["Job Rev Recognition Date"], row["Job Rev Recognition Dpt"] , row["Job Rev Recognition Date After Comma"] , row["Job Rev Recognition Dpt After Comma"] , row["Transaction_Date"],'No', row["Rev Recognition Month"])
                                cursor = conn.cursor()
                                cursor.execute(sql,params)
                                print(index , "Data Updated and  - Row Inserted")
                                conn.commit()
                                
                    conn.commit()
                    cursor = conn.cursor()
                    sql_status_update = "insert into [dbo].[FTP_Automation_Status]([Process_Name] , [Status] , [Delete_status]) values('REVENUE FTP AUTOMATION','NYC Data Updated','No')"
                    cursor.execute(sql_status_update)
                    conn.commit()
                    
                except Exception as ex:
                    cursor = conn.cursor()
                    sql_status_update = "insert into [dbo].[FTP_Automation_Status]([Process_Name] , [Status] , [Delete_status]) values('REVENUE FTP AUTOMATION','NYC Data Not Updated Kindly do it Manually','No')"
                    cursor.execute(sql_status_update)
                    conn.commit()
            else:
                cursor = conn.cursor()
                sql_status_update = "insert into [dbo].[FTP_Automation_Status]([Process_Name] , [Status] , [Delete_status]) values('REVENUE FTP AUTOMATION','NYC Data Not Updated (Due to File not Founded) Kindly do it Manually','No')"
                cursor.execute(sql_status_update)
                conn.commit()
        else:
            pass

        if "AUK Job Profit" in i:
            if today_date in i:
                try:
                    file_name = i
                    datem = datetime.today().strftime("%Y-%m")
                    month = datetime.today().strftime("%Y-%m")
                    df = pd.read_csv(r""+str(file_name)+"")
                    df = df.dropna(how='all')
                    df['Segments'] = df.apply(data_fil, axis=1)
                    df_date_check = pd.read_sql("select * from revenue_data_uk where CONVERT(VARCHAR, Transaction_Date)='"+month+"'",conn)
                    print(df_date_check)
                    df_filterd = df[["Job #","Job Local Ref","Job Brn.","Job Dept","Cont","Segments","Job Stat","Rep","SalesRep Name","Ops","Ops Name","Controlling Customer","Controlling Customer Roll","Job Rev Recognition","Consignee Full Name","House Bill Number","Job Local Client AR Settlement Group Full Name","Job Overseas Agent AR Settlement Group Full Name","Job Local Client AR Settlement Group Code","Consignor/Shipper/Supplier Full Name","Total Income","Cost","Accrual","Total Expense","Job Profit", "Rev Recognition Month"]]
                    df_filterd = df[["Job #","Job Local Ref","Job Brn.","Job Dept","Cont","Segments","Job Stat","Rep","SalesRep Name","Ops","Ops Name","Controlling Customer","Controlling Customer Roll","Job Rev Recognition","Consignee Full Name","House Bill Number","Job Local Client AR Settlement Group Full Name","Job Overseas Agent AR Settlement Group Full Name","Job Local Client AR Settlement Group Code","Consignor/Shipper/Supplier Full Name","Total Income","Cost","Accrual","Total Expense","Job Profit", "Rev Recognition Month"]]
                    df_filterd["Job Rev Recognition Before Comma"] = df_filterd["Job Rev Recognition"].str.split(',').str[0]
                    df_filterd["Job Rev Recognition After Comma"] = df_filterd["Job Rev Recognition"].str.split(',').str[1]
                    df_filterd["Job Rev Recognition Date"] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[1]
                    df_filterd["Job Rev Recognition Dpt"] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[0]
                    df_filterd = df_filterd.astype(str)
                    df_filterd["Job Rev Recognition Date After Comma"] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[0]
                    df_filterd["Job Rev Recognition Dpt After Comma"] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[1]
                    df_filterd["Job Local Ref"] = df_filterd["Job Local Ref"].str.replace(',','').astype(float)
                    df_filterd["Total Income"] = df_filterd["Total Income"].str.replace(',','').astype(float)
                    df_filterd["Cost"] = df_filterd["Cost"].str.replace(',','').astype(float)
                    df_filterd["Accrual"] = df_filterd["Accrual"].str.replace(',','').astype(float)
                    df_filterd["Total Expense"] = df_filterd["Total Expense"].str.replace(',','').astype(float)
                    df_filterd["Job Profit"] = df_filterd["Job Profit"].str.replace(',','').astype(float)
                    df_filterd["Job Rev Recognition Date After Comma"] = df_filterd["Job Rev Recognition Date After Comma"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = df_filterd["Job Rev Recognition Date"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = pd.to_datetime(df_filterd["Job Rev Recognition Date"])
                    df_filterd["Job Rev Recognition Date After Comma"] = pd.to_datetime(df_filterd["Job Rev Recognition Date After Comma"])
                    df_filterd["Job Rev Recognition Date After Comma"] = df_filterd["Job Rev Recognition Date After Comma"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = df_filterd["Job Rev Recognition Date"].fillna("1970-01-01")
                    df_filterd["Job Rev Recognition Date"] = pd.to_datetime(df_filterd["Job Rev Recognition Date"])
                    df_filterd["Job Rev Recognition Date After Comma"] = pd.to_datetime(df_filterd["Job Rev Recognition Date After Comma"])
                    df_filterd["Rev Recognition Month"] = pd.to_datetime(df_filterd["Rev Recognition Month"], format="%b-%y")
                    df_filterd["Transaction_Date"] = month
                    df_filterd = df_filterd.fillna("")

                    if month == datem:
                        if len(df_date_check) > 0:
                            sql_del = "DELETE FROM revenue_data_uk where CONVERT(VARCHAR, Transaction_Date)='"+month+"'"
                            cursor = conn.cursor()
                            cursor.execute(sql_del)
                            conn.commit()
                            for index,row in df_filterd.iterrows():
                                sql = "insert into revenue_data_uk( [Job #] , [Job Local Ref] , [Job Brn] , [Job Dept] , [Cont] , [Segments] , [Job Stat] , [Rep] , [SalesRep Name] , [Ops] , [Ops Name] , [Controlling Customer] , [Controlling Customer Roll] ,[Job Rev Recognition] ,[Consignee Full Name] , [House Bill Number] , [Job Local Client AR Settlement Group Full Name] , [Job Overseas Agent AR Settlement Group Full Name] , [Consignor Shipper Supplier Full Name] ,[Total Income], [Cost], [Accrual], [Total Expense], [Job Profit], [Job Rev Recognition Date], [Job Rev Recognition Dpt] , [Job Rev Recognition Date After Comma] , [Job Rev Recognition Dpt After Comma] , [Transaction_Date], [Delete_status], [Rev Recognition Month]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                params = (row["Job #"] , row["Job Local Ref"] , row["Job Brn."] , row["Job Dept"] , row["Cont"] , row["Segments"] , row["Job Stat"] , row["Rep"] , row["SalesRep Name"] , row["Ops"] , row["Ops Name"] , row["Controlling Customer"] , row["Controlling Customer Roll"] ,row["Job Rev Recognition"] ,row["Consignee Full Name"] , row["House Bill Number"] ,  row["Job Local Client AR Settlement Group Full Name"] , row["Job Overseas Agent AR Settlement Group Full Name"] ,row["Consignor/Shipper/Supplier Full Name"] ,row["Total Income"], row["Cost"], row["Accrual"], row["Total Expense"], row["Job Profit"], row["Job Rev Recognition Date"], row["Job Rev Recognition Dpt"] , row["Job Rev Recognition Date After Comma"] , row["Job Rev Recognition Dpt After Comma"] , row["Transaction_Date"],'No',row["Rev Recognition Month"])
                                cursor = conn.cursor()
                                cursor.execute(sql,params)
                                print(index , " - Data Deleted and Row Inserted")
                                conn.commit()
                        else:
                            for index,row in df_filterd.iterrows():
                                sql = "insert into revenue_data_uk( [Job #] , [Job Local Ref] , [Job Brn] , [Job Dept] , [Cont] , [Segments] , [Job Stat] , [Rep] , [SalesRep Name] , [Ops] , [Ops Name] , [Controlling Customer] , [Controlling Customer Roll] ,[Job Rev Recognition] ,[Consignee Full Name] , [House Bill Number] , [Job Local Client AR Settlement Group Full Name] , [Job Overseas Agent AR Settlement Group Full Name] , [Consignor Shipper Supplier Full Name] ,[Total Income], [Cost], [Accrual], [Total Expense], [Job Profit], [Job Rev Recognition Date], [Job Rev Recognition Dpt] , [Job Rev Recognition Date After Comma] , [Job Rev Recognition Dpt After Comma] , [Transaction_Date], [Delete_status], [Rev Recognition Month]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                params = (row["Job #"] , row["Job Local Ref"] , row["Job Brn."] , row["Job Dept"] , row["Cont"] , row["Segments"] , row["Job Stat"] , row["Rep"] , row["SalesRep Name"] , row["Ops"] , row["Ops Name"] , row["Controlling Customer"] , row["Controlling Customer Roll"] ,row["Job Rev Recognition"] ,row["Consignee Full Name"] , row["House Bill Number"] ,  row["Job Local Client AR Settlement Group Full Name"] , row["Job Overseas Agent AR Settlement Group Full Name"] ,row["Consignor/Shipper/Supplier Full Name"] ,row["Total Income"], row["Cost"], row["Accrual"], row["Total Expense"], row["Job Profit"], row["Job Rev Recognition Date"], row["Job Rev Recognition Dpt"] , row["Job Rev Recognition Date After Comma"] , row["Job Rev Recognition Dpt After Comma"] , row["Transaction_Date"],'No', row["Rev Recognition Month"])
                                cursor = conn.cursor()
                                cursor.execute(sql,params)
                                print(index , " -Data Not Deleted and  Row Inserted")
                                conn.commit()
                    else:
                        sql_del = "Update revenue_data_uk SET Delete_status='Yes' where CONVERT(VARCHAR, Transaction_Date)='"+month+"'"
                        cursor = conn.cursor()
                        cursor.execute(sql_del)
                        conn.commit()
                        for index,row in df_filterd.iterrows():
                                sql = "insert into revenue_data_uk( [Job #] , [Job Local Ref] , [Job Brn] , [Job Dept] , [Cont] , [Segments] , [Job Stat] , [Rep] , [SalesRep Name] , [Ops] , [Ops Name] , [Controlling Customer] , [Controlling Customer Roll] ,[Job Rev Recognition] ,[Consignee Full Name] , [House Bill Number] , [Job Local Client AR Settlement Group Full Name] , [Job Overseas Agent AR Settlement Group Full Name] , [Consignor Shipper Supplier Full Name] ,[Total Income], [Cost], [Accrual], [Total Expense], [Job Profit], [Job Rev Recognition Date], [Job Rev Recognition Dpt] , [Job Rev Recognition Date After Comma] , [Job Rev Recognition Dpt After Comma] , [Transaction_Date], [Delete_status], [Rev Recognition Month]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                params = (row["Job #"] , row["Job Local Ref"] , row["Job Brn."] , row["Job Dept"] , row["Cont"] , row["Segments"] , row["Job Stat"] , row["Rep"] , row["SalesRep Name"] , row["Ops"] , row["Ops Name"] , row["Controlling Customer"] , row["Controlling Customer Roll"] ,row["Job Rev Recognition"] ,row["Consignee Full Name"] , row["House Bill Number"] ,  row["Job Local Client AR Settlement Group Full Name"] , row["Job Overseas Agent AR Settlement Group Full Name"] ,row["Consignor/Shipper/Supplier Full Name"] ,row["Total Income"], row["Cost"], row["Accrual"], row["Total Expense"], row["Job Profit"], row["Job Rev Recognition Date"], row["Job Rev Recognition Dpt"] , row["Job Rev Recognition Date After Comma"] , row["Job Rev Recognition Dpt After Comma"] , row["Transaction_Date"],'No', row["Rev Recognition Month"])
                                cursor = conn.cursor()
                                cursor.execute(sql,params)
                                print(index , "Data Updated and  - Row Inserted")
                                conn.commit()
                                
                    conn.commit()
                    cursor = conn.cursor()
                    sql_status_update = "insert into [dbo].[FTP_Automation_Status]([Process_Name] , [Status] , [Delete_status]) values('REVENUE FTP AUTOMATION','AUK Data Updated','No')"
                    cursor.execute(sql_status_update)
                    conn.commit()
                    
                except Exception as ex:
                    cursor = conn.cursor()
                    sql_status_update = "insert into [dbo].[FTP_Automation_Status]([Process_Name] , [Status] , [Delete_status]) values('REVENUE FTP AUTOMATION','AUK Data Not Updated Kindly do it Manually','No')"
                    cursor.execute(sql_status_update)
                    conn.commit()
            else:
                cursor = conn.cursor()
                sql_status_update = "insert into [dbo].[FTP_Automation_Status]([Process_Name] , [Status] , [Delete_status]) values('REVENUE FTP AUTOMATION','AUK Data Not Updated (Due to File not Founded) Kindly do it Manually','No')"
                cursor.execute(sql_status_update)
                conn.commit()
        else:
            pass