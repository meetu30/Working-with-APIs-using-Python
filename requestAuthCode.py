# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 09:17:55 2021
This is step 1, right after you create developer's account, you create an app
for this app, you are given client id and secret key
Using those credentials, you make an API call, shown below, to request authorization Code
You save that code in auth.txt File and approach the admin, who will input this code in the app 
and admin will give you same "authorization Code" + "Refresh Token", which he obtained using aCode

create developer account --> create app --> get client id and secret key -->
Use them to get authorization code (this script) --> approach admin, who will give you 
REFRESH TOKEN (validity of 365 days) based on this aCode --> 
use that refresh token to get access token (expires every 59 days; AUTOMATION DONE at this level) 

@author: meetu
"""

import requests
from urlib.parse import quote
import yaml
import urllib3
urllib3.diable_warnings()

with open('config.yml', 'r') as f:
    conf = yaml.load(f, Loader=yaml.FullLoader)
    
client_id = conf.get("client_id")
client_secret = conf.get("client_secret")
redirect_url = conf.get("redirect_url")
company_id = conf.get("company_id")

redirect_urlen = quote(redirect_url, safe = '')

a= requests.get("https://" + client_id + "&redirect_uri=" + redirect_urlen + "&state....", verify = False)

with open('auth.txt', 'a') as f:
    f.write(str(a.url))
    

