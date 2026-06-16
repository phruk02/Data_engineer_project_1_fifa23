import requests
url = "https://api.massive.com/v3/reference/dividends?apiKey=ADZB_ohwXgNMC2xDAGZt9gXhneAvOkSn"

res = requests.get(url) 
data = res.json()


import pandas as pd

df = pd.json_normalize(data['results'])
print(df.head())

len(df)
df.info()