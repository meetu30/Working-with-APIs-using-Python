"""
Automate access token creation process for linkedIn
Based on 3 credentials - 1. refresh token (validity of 365 days), 2. client_id and 3. client_secret (created during app creation),
API is called to generate new access token (validity of 60 days)
This script will automate that process, so that Access token is automatically generated every 59th day,
starting from the day refresh token was created with 1 year validity

references: https://docs.microsoft.com/en-us/linkedin/shared/authentication/client-credentials-flow

@author: meetu
"""
from datetime import datetime, timedelta
import requests
import yaml
import json
import urllib3
import sys
urllib3.disable_warnings()

# Extract today's date
x = datetime.today()
dt = str(datetime.now() - timedelta(0)).split()
print(dt)
# Output is a list
dateToday = dt[0]

# create a log file for today
log = open("log_getToken_" + dateToday + ".txt", "a+" )

# Read refresh token creation date from backend config file or directly hardcode it here
refreshTokenCreationDate = "2021-02-10 01:01:01.000000"
startDate = datetime.strptime(refreshTokenCreationDate, "%Y-%m-%d %H:%M:%S.%f")

# get next refresh token date, which will be after an year to check if RT has expired
nextRefreshTokenDate = startDate + timedelta(days = 365)
print(nextRefreshTokenDate)

if(dateToday == nextRefreshTokenDate):
    log.write("RefreshToken has already expired!")
    sys.exit(0) # exits from the script


# create an empty list - dateList with 6 elements
dateList = []
n = 6

# while loop to add 6 dates to the dateList
while n:
    futureDate = startDate + timedelta(days = 59) 
    tnow = str(futureDate - timedelta(0)).split()
    dateList.append(tnow[0])
    
    startDate = futureDate
    n = n-1

print(dateList) 
# for example, if refreshToken is generated on- 2021-02-10, so every 2 months from this date, we need to request new Access Token
# So, we will call API on below 6 dates
#['2021-04-10', '2021-06-08', '2021-08-06', '2021-10-04', '2021-12-02', '2022-01-30']

# defining a function to call API to generate new token
def callAPI():
    with open('config.yml', 'r') as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    
    client_id = conf.get("client_id")
    client_secret = conf.get("client_secret")
    rCode = conf.get("refreshToken")

    #redirect_url = conf.get("redirect_url")
    #company_id = conf.get("company_id")
    oldAccessToken = conf.get("AccessToken")
    
    # create a dictionary - "parameters", which will have above 3 credentials
    Parameters = {'grant_type' : 'refresh_token',
                  'refresh_token' : rCode,
                  'client_id' : str(client_id),
                  'client_secret' : str(client_secret)
                  }
    
    # pass the above credentials to API call
    resp = requests.post('https://www.linkedin.com/oauth/v2/accesstoken', params = Parameters, verify = False)
    
    # storing API response in the form of JSON format
    response = resp.json()
    
    # check the API connection was successful 
    print(resp.status_code)
    
    # to check all tags in json response
    print(resp.json().keys)
    
    # to print json response in indented format for better readability
    print(json.dumps(resp.json(), indent = 4))
    
    # Extract new token from the above JSON format 
    # and save it to a new variable - newAccessToken
    newAccessToken = response['access_token']
    
    # replace oldAccessToken with newAccessToken
    with open('config.yml', 'r') as file:  # READ mode
        fileData = file.read()
        fileData = fileData.replace(oldAccessToken, newAccessToken)
    with open('config.yml', 'w') as file: # WRITE mode
        file.write(fileData)
        

# Check dateList elements if there is any match with today's date
for i in dateList:
    if dateToday == i:
        callAPI()
        log.write("\n New token Successfully generated!")
 
   

