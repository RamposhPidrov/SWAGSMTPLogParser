import requests as rq 
import json
#http://free.ipwhois.io/json/*тут айпишник*
r = rq.get('http://ip-api.com/json/24.48.0.1', params=None)
data = json.loads(r.content)
print(data["country"])