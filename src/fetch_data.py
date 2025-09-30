import requests
import pandas as pd
import json
from creds import client_id, client_secret, username, password, user_agent



auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
data = {"grant_type": "password", "username": username, "password": password}
headers = {"User-Agent": user_agent}

res = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data, headers=headers)

token = res.json()["access_token"]
headers = {**headers, **{"Authorization": f"bearer {token}"}}

res = requests.get("https://oauth.reddit.com/r/astronomy/top",
                   headers=headers, params={"limit": 5})

df = pd.DataFrame(res.json()["data"]["children"])
print(df.head())    


""" 

with open("data.json", "w") as json_file:
        json.dump(res.json, json_file, indent=4)
        print("Data appended to data.json file.") """