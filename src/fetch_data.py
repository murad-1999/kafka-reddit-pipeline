import requests
import praw
import pandas as pd
from creds import client_id, client_secret, username, password, user_agent



auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
data = {"grant_type": "password", "username": username, "password": password}
headers = {"User-Agent": user_agent}

res = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data, headers=headers)
print(res.json())

""" token = res.json()["access_token"]
headers = {**headers, **{"Authorization": f"bearer {token}"}}

res = requests.get("https://oauth.reddit.com/r/astronomy/top",
                   headers=headers, params={"limit": 500})
for post in res.json()["data"]["children"]:
    print(post["data"]["title"])


 """


