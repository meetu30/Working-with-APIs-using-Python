"""
########## Automate Access Token generation
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
import pandas as pd

################ Create a log file for today ##############
tnow = str(datetime.now() - timedelta(0)).split()
datetimenow = tnow[0] + '_' + '(' + tnow[1].split('.')[0] + ')'
datetimenow = datetimenow.replace(':', '_')
log = open("log_" + datetimenow + ".txt", "a+")

############### Load config file to capture all credentials ############
try:
    with open("config.yml", "r") as dbconf:
        dbconfig = yaml.load(dbconf, Loader = yaml.FullLoader)
        
    
    old_accessToken = dbconfig.get("ACCTOK") #read old Access Token
    apiKey = dbconfig.get("apiKey")
    ORG_ID =  dbconfig.get("orgID")
    globalCompanyID = dbconfig.get("globalCompanyId")
    rsID = dbconfig.get("rsID")
    CLIENT_ID = dbconfig.get('your client id')
    CLIENT_SECRET = dbconfig.get('your client secret')
    SUB_ID = dbconfig.get('your technical account ID') 
    

    log.write("Credentials loaded successfully \n")  
except Exception as e:
    log.write("Error occured while loading DB credentials")

##########################################################################

JWT_URL = 'https://ims-na1.adobelogin.com/ims/exchange/jwt/'

# read private key here
with open('your path to/private.key') as file:
    private_key = file.read()

# this is from Adobe Developer Console with true changed to True
jwt_payload = {"iss": ORG_ID,   #"youriss@AdobeOrg"
                "sub":SUB_ID,   #"yoursub@techacct.adobe.com"
                "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk":True,
                "aud":"https://ims-na1.adobelogin.com/c/CLIENT_ID"}

# generate an expiry date for JWT Token +120 minutes
jwt_payload['exp'] = (datetime.datetime.utcnow() + datetime.timedelta(minutes=120)
                      
# create another payload that we'll trade for our access key
access_token_request_payload = {'client_id': f'{CLIENT_ID}',
                            'client_secret': f'{CLIENT_SECRET}'}

# encrypt the jwt_payload with our private key
jwt_payload_encrypted = jwt.encode(jwt_payload, private_key, algorithm='RS256')

# add this encrypted payload to our token request payload
# decode makes it a string instead of a bytes file
access_token_request_payload['jwt_token'] = jwt_payload_encrypted.decode('UTF-8')

# to avoid HTTP certificate error set session verify to False
session = requests.Session()
session.verify = False
# make the post request for our access token
# for this to work we need to use "data=" to generate the right headers
# using json= or files= won't work
response = session.post(url=JWT_URL, data=access_token_request_payload)

response_json = response.json()
# set our access token
new_accessToken = response_json['access_token']

# response_json looks like this:
#{'token_type': 'bearer',
# 'access_token': 'randomLettersSymbolsAndNumbers',
# 'expires_in': 86399998}

# replace old token with this new token in config file
with open("config.yml", "r") as file:
    fileData = file.read()

fileData = fileData.replace(old_accessToken, new_accessToken)

with open(str("config.yml", "w") as file:
    file.write(fileData)     
