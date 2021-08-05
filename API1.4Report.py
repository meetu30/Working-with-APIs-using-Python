import time
from datetime import datetime,timedelta
import requests, yaml
import re
import sys 
import pyodbc
from urllib.parse import quote
import urllib3 
urllib3.disable_warnings()
yaml.warnings({ 'YAML Loadwarning': False})
import json
import yaml

import pandas as pd
import numpy as np

################ Create a log file for today ##############
tnow = str(datetime.now() - timedelta(0)).split()
datetimenow = tnow[0] + '_' + '(' + tnow[1].split('.')[0] + ')'
datetimenow = datetimenow.replace(':', '_')
log = open("log_" + datetimenow + ".txt", "a+")

############### Load config file to capture all credentials ############
try:
    with open("config.yml", "r") as dbconf:
        dbconfig = yaml.load(dbconf, Loader = yaml.FullLoader)
        
    UID = dbconfig.get("UID")
    PWD = dbconfig.get("PWD")
    DataBase = dbconfig.get("DATABASE")
    Server = dbconfig.get("SERVER")
    
    td_var = dbconfig.get("TD_VAR")
    accessToken = dbconfig.get("ACCTOK")
    apiKey = dbconfig.get("apiKey")
    orgID =  dbconfig.get("orgID")
    GLOBAL_COMPANY_ID = dbconfig.get("globalCompanyId")
    rsID = dbconfig.get("rsID")
    segmentID = dbconfig.get("segmentID")
    segmentName = dbconfig.get("segmentName")
    CLIENT_ID = dbconfig.get('your client id')
    CLIENT_SECRET = dbconfig.get('your client secret')
    

    log.write("Credentials loaded successfully \n")  
except Exception as e:
    log.write("Error occured while loading DB credentials")
    
################# fetch today's and yesterday's time #############
tyesterday = str(datetime.now() - timedelta(td_var + 1)).split(" ")
today = str(datetime.now() - timedelta(td_var)).split(" ")[0]
yesterday = str(datetime.now() - timedelta(td_var + 1)).split(" ")[0]

############### get time range in API format #################
range_from = tyesterday[0] + "T00:00:00.000"
range_to = today[0] + "T00:00:00.000"

############## Define header with all credentials ###########
# Our header now contains everything we need for API calls - AT, clientId and GlobalCompanyId
HEADER = {
    'Accept':'application/json',
    'Authorization':f'Bearer {accessToken}',
    'x-api-key':CLIENT_ID,
    'x-proxy-global-company-id': GLOBAL_COMPANY_ID,
}

# URLs used for Reporting API
POST_RQ_STEP1 = 'https://api5.omniture.com/admin/1.4/rest/?method=Report.Queue'
POST_RG_STEP2 = 'https://api5.omniture.com/admin/1.4/rest/?method=Report.Get'

# Define our variables - METS list has all variables that we want to fetch
# ELEMS has page levels or dimensions and BODY has all credentials
METS = [{'id':'pageviews'},
        {'id':'visits'},
        {'id':'visitors'},
        {'id':'revenue'},
        {'id':'bouncerate'}]

ELEMS = [{'id':'evar007','top':50000}, # max elements supported is 50k
        {'id':'evar420','top':50000}, # if we put a higher number here, the API will complain
        {'id':'product','top':50000}]

# the body of our post request
BODY = {
    'reportDescription':{
        'dateGranularity':'day', # gives us a daily date column
        'anomalyDetection':False,
        'curretData':True,
        'dateFrom':'2021-01-01',
        'dateTo':'2021-01-10',
        'reportSuiteID':RSID,
        'elementDataEncoding':'utf8',
        'metrics':METS,
        'elements':ELEMS,
        'expedite':False
    }
}

# The first URL defined above will help us connect to server and will return reportID
session = requests.Session()
session.verify = False
r1 = session.post(url=POST_RQ_STEP1,headers=HEADER,json=BODY)

# r1.json() Print it in JSON format and you can see that reportID, which will be used in step 2 to get actual data for metrices defined in METS
# {'reportID': 1234567890}

# give us a timestamp
def timestamp():
    return datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
  
# use formula 2*(4^n) for backoff, which will define the sleep time between consecutive requests
# 8 sec, 32sec, 124sec (~2min), ~8min, ~34min [will never exceed 5 steps in cloud run (60min max run time)]
def try_with_backoff(url,header,payload):
    print(f'{timestamp()} trying request')
    req = requests.post(url=url,headers=header,json=payload)
    n = 1
    sleep_base = 4
    while (req.status_code != 200): # and (n<=5):
        sleep_time = 2*(sleep_base**n)
        sleep_time_mins = round(sleep_time/60,2)
        print(f'{timestamp()} req.status = {req.status_code} -- sleep for {sleep_time} secs ~ {sleep_time_mins} mins')
        time.sleep(sleep_time)
        print(f'{timestamp()} retry request')
        req = requests.post(url=url,headers=header,json=payload)
        n += 1
    if(req.status_code != 200):
        return f'error -- {req.text}'
    
    return req

# our second post request
r2 = try_with_backoff(url=POST_RG_STEP2, header=HEADER, payload=r1.json())

  
# explode the number of levels there are and create a DF
def explode_n_concat(df,col_name,elems,mets):
    # put info into variables
    metrics = [x['name'] for x in mets]
    dimensions = [x['name'] for x in elems]
    
    # need to loop one more time than we have elements
    loops = len(elems)
    for i in range(0,loops):
        # base case change the col names to show level0
        if i == 0:
            df.columns = df.columns + f'_{i}'
            df['date'] = pd.to_datetime(df['name_0'])
            
        # explode the breakdown
        df = df.explode(col_name+f'_{i}').reset_index(drop=True)
        # put the breakdown in a new dataframe
        df_temp = pd.DataFrame.from_records(df[col_name+f'_{i}'].dropna().tolist())
        # rename the columns to add level related information
        df_temp.columns = df_temp.columns + f'_{i+1}'
        # change the name of the name columns to match the elements
        df_temp.rename(columns={f'name_{i+1}':dimensions[i]},inplace=True)
        # stitch the new columns onto our existing df
        df = pd.concat([df,df_temp],axis='columns')
        # rename the metrics columns
        if i == loops-1:
            df_mets = pd.DataFrame(df[f'counts_{i+1}'].apply(lambda x: fill_nan(x,metrics)).to_list(),columns=metrics)
            df = pd.concat([df,df_mets],axis='columns')
    
    # last bit of cleaning up
    cols = ['date']
    cols.append(dimensions)
    cols.append(metrics)
    selected_cols = list(pd.core.common.flatten(cols))
    df[metrics] = df[metrics].astype(float)
    return df[selected_cols]

# request cleaner using our other function explode_n_concat
def req_2_df(req):
    df = pd.json_normalize(req.json(),['report','data'])
    elems = req.json()['report']['elements']
    mets = req.json()['report']['metrics']
    df_clean = explode_n_concat(df=df,col_name='breakdown',elems=elems,mets=mets)
    return df_clean
  
df = req_2_df(r2)

   
finalList = df.values.tolist()  # converting DF to flatList with each element representing data for all dimensions, which will be pushed into database

############## Set up connection to database ##############
try:
    connection = pyodbc.connect("Driver = {ODBC Driver 13 for SQL Server};"
                                "Server =" +Server+";"
                                "Database = " +DataBase+ ";"
                                "uid = " +UID+ ";"
                                "password = " +PWD,char_encoding = 'utf-8',
                                wchar_encoding = "utf-161e")
    
    cursor = connection.cursor()
    log.write("DB connection Successful\n")
    
except Exception as e:
    log.write("Error occurred while connecting to backend DB: " + str(e))


############## Inserting data to the backend table ##############
try:
    
    ##### Check if the data exists for the given date
    cursor.execute("SELECT date FROM GIVE_TABLE_NAME WHERE data = ?", yesterday)
    data_lookup = []
    for record in cursor.fetchall():
        data_lookup.append(str(record))
        
    ##### if data exists then come out of the script, else insert data    
    if data_lookup:
        log.write("Data already exists in the table!!")
        sys.exit(0)
    
    #### Insert data to the given table
    cursor.executemany("INSERT INTO GIVE_TABLE_NAME(segmentID, page, visits, uniqueVisitors, revenue, bouncerate, date) VALUES (?,?,?,?,?,?,?);", finalList)
    log.write("Data inserted to GIVE_TABLE_NAME table successfully!")

except Exception as e:
    log.write("Error occurred while pushing data to DB: " + str(e))
    
connection.commit()
connection.close()
log.close()

###################### End of file #####################

        
    

