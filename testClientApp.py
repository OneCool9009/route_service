import json
import requests

SERVICE_URL_POST = 'http://localhost:5001/cluster/calculate'
SERVICE_URL_GET = 'http://localhost:5001/cluster/getCalculation'

# file = {'file': open('graph_data.xlsx', 'rb')}
# responce = requests.post(SERVICE_URL_POST, files=file)
# print(json.loads(responce.content))

responce = requests.get(SERVICE_URL_GET)
print(json.loads(responce.content))
