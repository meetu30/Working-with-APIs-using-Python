
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

############# optional step: saving json for level 1 data in local machine
parsed_json = json.loads(resp.text)
with open('C:/Users/mx007/Desktop/myJsonFile.json', 'w') as f:
    json.dump(parsed_json, f, indent = 4, sort_keys=True, separators = (",", ":"))

############# Save all itemIds in a list ##################
items = []
itemValue = []

for d in response['rows']:
    itemId = d['itemId']
    canonical_URL = d['value']
    
    items.append(itemId)
    itemValue.append([itemId, canonical_URL])
    
print(len(itemValue)) # you get these many item Ids at level 1

############# Function to make API call and capture all metrices for the ItemIds captured at level 1
def getMetrics(i,v):
    
    body = {
    'rsid': rsID,                 #yourReportSuiteID
    'globalFilters': [{
    'type': 'dateRange',
    'dateRange': range_from + "/" + range_to
    },{
       'type': 'segment',
       'segmentId': segmentID
       }],
    'metricContainer': {
    'metrics': [
        
    {
     "columnId": "0",
     "id": "metrics/pageviews",
     "filters": [
         "0"
         ]
     },
    
    {
     "columnId": "1",
     "id": "metrics/visits",
     "filters": [
         "0"
         ]
     },
    
    {
     "columnId": "3",
     "id": "metrics/visitors",
     "filters": [
         "0"
         ]
     },
    
    {
     "columnId": "4",
     "id": "metrics/revenue",
     "filters": [
         "0"
         ]
     },
    
    {
     "columnId": "5",
     "id": "metrics/bouncerate",
     "filters": [
         "0"
         ]
     },
    
    ],
    "metricFilters": [
        {
            "id": "0",
            "type":"breakdown",
            "dimension": "variables/daterangeday",
            "itemId": i
            }
               
        ]
    
    },
    
    
    'dimension': 'variables/evar2', # It could be any dimension such as evar50/evar2 etc.
    'settings': {
    'dimensionSort': 'asc',
    'limit': '10000'
    },
    "statistics":{
        "functions": [
            "col-max",
            "col-min"
            ]
        }
    
    }
    
    endpoint = "https://analytics.adobe.io/api/" + globalCompanyID + "/reports"
    
    respLevel2 = requests.post(endpoint, json = body, headers = header, verify = False)
    response1 = respLevel2.json()
    print(json.dumps(respLevel2.json(), indent = 4))
    
    
    ############# optional step: saving json for level 2 data in local machine
    parsed_json = json.loads(respLevel2.text)
    with open('myJsonFile_level2.json', 'w') as f:
        json.dump(parsed_json, f, indent = 4, sort_keys=True, separators = (",", ":"))
    
    ############# create a list which will fetch all metrics values
    level2_details = []
    
    for d in response['rows']:
        page = d['values']
        pageViews = int(d['data'][0])
        visits = int(d['data'][1])
        uniqueVisitors = int(d['data'][2])
        revenue = round(float(d['data'][3]),2)
        bounceRate = round((float(d['data'][3]) * 100 ),2) #convert it to percentage
        
        ###### append all values to the list
        level2_details.append([
                                segmentID,
                                page,
                                visits,
                                uniqueVisitors,
                                revenue,
                                bounceRate,
                                yesterday
            
                            ])
    
    return level2_details

################## Call API function above and store final values 
try:
    
    start = time.time()
    
    finalMetrics = []
    
    for i, v in itemValue:
        
        print("Capturing level 2 details for Item ID : " + str(i))
        b = getMetrics(i, v)
        
        finalMetrics.append(b)
        
        flatList = []
        for elem in finalMetrics:
            for item in elem:
                flatList.append(item)
        
        print(len(flatList))
        
    time.sleep(5) # to give break of 5 seconds between 2 requests
    end = time.time()
    print("Total time for level 2 API call is {end - start}")
    
    log.write("Details at level 2 fetched successfully \n")
    
except Exception as e:
    log.write("Error Occured while fetching details at level 2" + str(e))



        
    

