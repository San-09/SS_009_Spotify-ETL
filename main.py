import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
#import mysql.connector

# cnx = mysql.connector.connect(user="root", password="", host="3306", database="")
# MySQL_location = ""
User_id = "Santheep Sritharan"
token = "BQA2kWJ3txuGECBas6TTvvFl6ELMwrB3lXDkrVKkLzK4OH_6lbxV5k2a6G7pwLF0D9uzkoKWrCEE0LwQr3I-ojVFp4wFzy09pl9XFwZ-Tj7hao9e43HJUqBk58clrkFN7RMHFk2RAHfa18gnNyeKSWHsNkVbcNJrQJMkakAb"

if __name__ == "__main__":
    headers = {
        "Accept": "application/json",
        "Content_Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)}

    #Convert time to Unix timestamp in milliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp())*1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
        time = yesterday_unix_timestamp), headers = headers)

    data = r.json()

print(data)