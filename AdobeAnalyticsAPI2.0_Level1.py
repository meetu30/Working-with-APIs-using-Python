

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
with open('myJsonFile.json', 'w') as f:
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



