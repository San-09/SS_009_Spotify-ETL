import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
#import mysql.connector

#Pandas Settings: show more columns in terminal
pd.set_option('display.max_columns', 5)

# cnx = mysql.connector.connect(user="root", password="", host="3306", database="")
# MySQL_location = ""

User_id = "Santheep Sritharan"
token = "BQCWKb65I0jsWD_1rAoEl1FP_iVXj7m9ac_iGhKfigWaTo_tW6hU1XUfx6_wh9Vet5-pFojKtX8X1IYjezxG_28Cc_YXEPLuVk8j2SsoB0ptP7DTMTkQfhVlHGH72lvZ_Tv9mvWP86TMtWcmOCJyk0-XLoQGoQTuo8QvE6vp"

def validate_data(df: pd.DataFrame) -> bool:
    #Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    #Primary key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")

    #Check for nulls:
    if df.isnull().values.any():
        raise Exception("Null values found")


    #Check if timestamp are of pre-24hours
    yesterday = datetime.datetime.now()-datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

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
        time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

#print(data)

# # Save to text file for manual read of data/ Json Parser
# file = open("Spotify Data 2.txt", "a")
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

if validate_data(song_df):
    print("Data valid")

print(song_df)