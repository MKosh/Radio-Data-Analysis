from bs4 import BeautifulSoup
import requests
import json
import pandas as pd

source = requests.get('https://nowplaying.bbgi.com/WMMRFM/list?limit=200&amp;offset=0').text

information = json.loads(source)

#df = pd.read_json(information)
df = pd.DataFrame(information)
time_and_date = df['createdOn'][0]
date, time = time_and_date.split('T')
print("date = ",date,", time = ", time)
# =(((G1/60)/60)/24)+DATE(1970,1,1)-TIME(4,0,0)
df.to_csv("data.csv")

print("Done!")
# print(information[1]['title'])

