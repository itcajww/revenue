from django.shortcuts import render, HttpResponse
from django.shortcuts import redirect
from django.contrib.auth.models import User, auth
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
import statistics 
import json
import pandas as pd
from django.core.mail import EmailMultiAlternatives
from django.conf import settings
from django.core.mail import send_mail
import os
import glob
import schedule
import time
import xlrd
import pyodbc
from django.contrib.auth import get_user_model
from datetime import datetime
from django.core.files.storage import FileSystemStorage
pd.options.display.float_format = "{:,.0f}".format
from datetime import datetime

# Create your views here.
# for login page

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=192.168.0.117;'
                      'Database=BI;'
                      'UID=BI;'
                      'PWD=BI$%^app;'
                      'Trusted_Connection=no;'
                      )

def index(request):
    er_msg = ''
    if request.method == 'POST':
        username = request.POST['email'] #username
        password = request.POST['password'] #password
        user = authenticate(username=username, password=password)  # Authendicating user
        if user is not None:
            login(request,user)  # if user availlable login
            if request.user.username == 'support@ajww.com': # if support meand redirect to support dashboard
                return redirect('/data_load/')
            else:
                print("error1")
        else:
            print("error2")
            er_msg = 'True'
    context={'er_msg':er_msg}
    return render(request,'index.html',context)


def logout_view(request):  # Logout and redirect to login page
    logout(request)
    return redirect('/')


# for dashboard page
def dashboard(request):
    return render(request,'dashboard.html')

def data_load_ajax(request):
    if request.method == "POST":
        
        datem = datetime.today().strftime("%Y-%m")

        month = request.POST["month"]

        handle_uploaded_file(request.FILES["file_name"])
        # Reading Raw Data
        df = pd.read_excel('data_file.xlsx',engine='openpyxl')

        # Droping all Nan
        df = df.dropna(how='all')

        # Creating Df Segments
        df['Segments'] = df.apply(data_fil, axis=1)

        # 
        df_date_check = pd.read_sql("select * from revenue_data_important where CONVERT(VARCHAR, Transaction_Date)='"+month+"'",conn)

        # Creating Filterd Df
        df_filterd = df[["Job #","Job Local Ref","Job Brn.","Job Dept","Cont","Segments","Job Stat","Rep","SalesRep Name","Ops","Ops Name","Controlling Customer","Controlling Customer Roll","Job Rev Recognition","Consignee Full Name","House Bill Number","Job Local Client AR Settlement Group Full Name","Job Overseas Agent AR Settlement Group Full Name","Job Local Client AR Settlement Group Code","Consignor/Shipper/Supplier Full Name","Total Income","Cost","Accrual","Total Expense","Job Profit", "Rev Recognition Month"]]

        # Data Preprocessing for Revenue Data
        df_filterd[["Job Rev Recognition Before Comma"]] = df_filterd["Job Rev Recognition"].str.split(',').str[0]
        df_filterd[["Job Rev Recognition After Comma"]] = df_filterd["Job Rev Recognition"].str.split(',').str[1]
        df_filterd[["Job Rev Recognition Date"]] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[1]
        df_filterd[["Job Rev Recognition Dpt"]] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[0]
        df_filterd = df_filterd.astype(str)
        df_filterd[["Job Rev Recognition Date After Comma"]] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[0]
        df_filterd[["Job Rev Recognition Dpt After Comma"]] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[1]
        df_filterd["Job Local Ref"] = df_filterd["Job Local Ref"].astype(float)
        df_filterd["Total Income"] = df_filterd["Total Income"].astype(float)
        df_filterd["Cost"] = df_filterd["Cost"].astype(float)
        df_filterd["Accrual"] = df_filterd["Accrual"].astype(float)
        df_filterd["Total Expense"] = df_filterd["Total Expense"].astype(float)
        df_filterd["Job Profit"] = df_filterd["Job Profit"].astype(float)
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

        # Checking if the data is curent month data

        # Data Insertion Operation
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
    return render(request,'data_load.html')







def data_load_uk_ajax(request):
    if request.method == "POST":
        datem = datetime.today().strftime("%Y-%m")
        month = request.POST["month"]
        print(month)
        handle_uploaded_file(request.FILES["file_name"])
        
        # Data Reading for UK
        df = pd.read_excel('data_file.xlsx',engine='openpyxl')
        df = df.dropna(how='all')
        df['Segments'] = df.apply(data_fil, axis=1)
        df_date_check = pd.read_sql("select * from revenue_data_uk where CONVERT(VARCHAR, Transaction_Date)='"+month+"'",conn)

        # Data Filtering and preprocessing
        df_filterd = df[["Job #","Job Local Ref","Job Brn.","Job Dept","Cont","Segments","Job Stat","Rep","SalesRep Name","Ops","Ops Name","Controlling Customer","Controlling Customer Roll","Job Rev Recognition","Consignee Full Name","House Bill Number","Job Local Client AR Settlement Group Full Name","Job Overseas Agent AR Settlement Group Full Name","Job Local Client AR Settlement Group Code","Consignor/Shipper/Supplier Full Name","Total Income","Cost","Accrual","Total Expense","Job Profit", "Rev Recognition Month"]]
        df_filterd[["Job Rev Recognition Before Comma"]] = df_filterd["Job Rev Recognition"].str.split(',').str[0]
        df_filterd[["Job Rev Recognition After Comma"]] = df_filterd["Job Rev Recognition"].str.split(',').str[1]
        df_filterd[["Job Rev Recognition Date"]] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[1]
        df_filterd[["Job Rev Recognition Dpt"]] = df_filterd["Job Rev Recognition Before Comma"].str.split(' ').str[0]
        df_filterd = df_filterd.astype(str)
        df_filterd[["Job Rev Recognition Date After Comma"]] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[0]
        df_filterd[["Job Rev Recognition Dpt After Comma"]] = df_filterd["Job Rev Recognition After Comma"].str.split(' ').str[1]
        df_filterd["Job Local Ref"] = df_filterd["Job Local Ref"].astype(float)
        df_filterd["Total Income"] = df_filterd["Total Income"].astype(float)
        df_filterd["Cost"] = df_filterd["Cost"].astype(float)
        df_filterd["Accrual"] = df_filterd["Accrual"].astype(float)
        df_filterd["Total Expense"] = df_filterd["Total Expense"].astype(float)
        df_filterd["Job Profit"] = df_filterd["Job Profit"].astype(float)
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

        # Checking if the data is curent month data
        if month == datem:
            if len(df_date_check) > 0:
                # Deleting Data
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
    return render(request,'data_load.html')






def data_load(request):
    return render(request,'data_load.html')










def handle_uploaded_file(f):
    with open('data_file.xlsx', 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)


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


def View_data(request):
    df = pd.read_excel("data_file.xlsx")
    df = df.head(20)
    df = df.to_html(index=False)
    context = {"df":df}
    return render(request,'View_data.html',context)