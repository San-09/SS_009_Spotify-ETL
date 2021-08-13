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

# file = open("Spotify Data.txt", "a")
# file.write(str(data))
# file.close()

song_title = []
artist = []
release_date = []
played_date = []
timestamp = []

for song in data["items"]:
    song_title.append(song["track"]["name"])
    artist.append(song["track"]["artists"][0]["name"])
    release_date.append(song["track"]["album"]["release_date"]) #YYYY-MM-DD
    played_date.append(song["played_at"])
    timestamp.append(song["played_at"][0:10])

print("Song title array:", song_title)

#Convert dictionary into a pandas dataframe

song_dict = {
    "song_title": song_title,
    "artist_name": artist,
    "release_date": release_date,
    "played_at": played_date,
    "timestamp": timestamp
}
dict_columns = ["song_title", "artist_name", "release_date", "played_at", "timestamp"]

song_df = pd.DataFrame(song_dict, columns = dict_columns)

print(song_df)