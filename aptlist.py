from idpw import akey
import requests
import xmltodict
import json
import pandas as pd

expr_str = 'as1 =="서울특별시" '

url = 'http://apis.data.go.kr/1613000/AptListService2/getTotalAptList'

# totalCount
params ={'serviceKey' : akey,
         'pageNo' : '1', 
         'numOfRows' : '1' 
         }
headers = {'Set-Cookie': 'ROUTEID=.HTTP1; Path=/1613000/AptListService2; Domain=apis.data.go.kr', 
           'Access-Control-Allow-Origin': '*', 'Content-Encoding': 'gzip', 'Content-Type': 'application/xml', 
           'Content-Length': '836', 'Date': 'Sun, 09 Jul 2023 00:27:32 GMT', 'Server': 'NIA API Server'}

response = requests.get(url, params=params)

xml_data = xmltodict.parse(response.text)
json_data = json.loads(json.dumps(xml_data))
row = json_data['response']['body']['totalCount']
print(row)

totalCount = int(row)
numOfRows = 500
pages = int(totalCount / numOfRows) + 1
print(pages+1,row)

for x in range(1,pages+1):
    params ={'serviceKey' : akey,
            'pageNo' : str(x), 
            'numOfRows' : str(numOfRows)
         }
    response = requests.get(url, params=params) # ,headers=headers
    # xml_data = xmltodict.parse(response.text)
    xml_data = xmltodict.parse(response.content.decode('utf-8'))
    json_data = json.loads(json.dumps(xml_data))
    ord_json = json_data['response']['body']['items']['item']
    # print(x)
    if x == 1:
        df_total = pd.DataFrame(ord_json) # ,index=[0]
    else:
        tmp = pd.DataFrame(ord_json) #,index=[0]
        df_total = pd.concat([df_total,tmp])

print(len(df_total))
# print(df_apt)

df_total['sggcd'] = df_total['bjdCode'].str[:5] 
df_total.groupby(['sggcd','as1','as2']).agg({'as1':'count'})
df_total.query(expr_str).groupby(['sggcd','as1','as2']).size().to_frame('건').reset_index()
