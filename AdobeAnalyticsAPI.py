# -*- coding: utf-8 -*-
"""

Created on Mon Mar 22 17:21:45 2021

@author: meetu
"""

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
main_path = "C:/Users/meetu/Desktop"
import yaml

################ Create a log file for today ##############
tnow = str(datetime.now() - timedelta(0)).split()
datetimenow = tnow[0] + '_' + '(' + tnow[1].split('.')[0] + ')'
datetimenow = datetimenow.replace(':', '_')
log = open(str(main_path) + "/Adobe/log/log_" + datetimenow + ".txt", "a+")

############### Load config file to capture all credentials ############
try:
    with open(str(main_path) + "/Adobe/config/config.yml", "r") as dbconf:
        dbconfig = yaml.load(dbconf, Loader = yaml.FullLoader)
        
    UID = dbconfig.get("UID")
    PWD = dbconfig.get("PWD")
    DataBase = dbconfig.get("DATABASE")
    Server = dbconfig.get("SERVER")
    
    td_var = dbconfig.get("TD_VAR")
    accessToken = dbconfig.get("ACCTOK")
    apiKey = dbconfig.get("apiKey")
    orgID =  dbconfig.get("orgID")
    globalCompanyID = dbconfig.get("globalCompanyId")
    rsID = dbconfig.get("rsID")
    segmentID = dbconfig.get("segmentID")
    segmentName = dbconfig.get("segmentName")
    

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
header = {
 'Authorization': 'Bearer ' + accessToken,
 'x-api-key': apiKey,
 'x-gw-ims-org-id': orgID,
 'Accept': 'application/json',
 'x-proxy-global-company-id': globalCompanyID,
 'Content-Type': 'application/json'
 }

######## Define body, which will pass metrices to be collected and the filters/dimensions
body = {
'rsid': 'rsID',                 #yourReportSuiteID
'globalFilters': [{
'type': 'dateRange',
'dateRange': range_from + "/" + range_to
},{
   'type': 'segment',
   'segmentId': segmentID
   }],
'metricContainer': {
'metrics': [
    
{'id': 'metrics/pageviews'},   # fetch data for these 5 metrices
{'id': 'metrics/visits'},
{'id': 'metrics/visitors'},
{'id': 'metrics/revenue'},
{'id': 'metrics/bouncerate'}

]},
'dimension': 'variables/daterangeday', # It could be any dimension such as evar50/evar2 etc.
'settings': {
'dimensionSort': 'asc',
'limit': '10000'
}}
   
################ URL to make API call
apiCall = 'https://analytics.adobe.io/api/' + globalCompanyID + '/reports'
resp = requests.post(apiCall, json = body, headers = header, verify = False)
response = resp.json()

############# optional step: saving json in local machine
parsed_json = json.loads(resp.text)
with open('C:/Users/mx007/Desktop/myJsonFile.json', 'w') as f:
    json.dump(parsed_json, f, indent = 4, sort_keys=True, separators = (",", ":"))

############# create a list which will fetch all metrics values
page_details = []

for d in response['rows']:
    page = d['values']
    pageViews = int(d['data'][0])
    visits = int(d['data'][1])
    uniqueVisitors = int(d['data'][2])
    revenue = round(float(d['data'][3]),2)
    bounceRate = round((float(d['data'][3]) * 100 ),2) #convert it to percentage
    
    ###### append all values to the list
    page_details.append([
                            segmentID,
                            page,
                            visits,
                            uniqueVisitors,
                            revenue,
                            bounceRate,
                            yesterday
        
                        ])

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
    cursor.executemany("INSERT INTO GIVE_TABLE_NAME(segmentID, page, visits, uniqueVisitors, revenue, bouncerate, date) VALUES (?,?,?,?,?,?,?);", page_details)
    log.write("Data inserted to GIVE_TABLE_NAME table successfully!")

except Exception as e:
    log.write("Error occurred while pushing data to DB: " + str(e))
    
connection.commit()
connection.close()
log.close()

###################### End of file #####################

        
    

