import pandas as pd
from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import mysql.connector
from mysql.connector import errorcode


#Pandas Settings: show more columns in terminal
pd.set_option('display.max_columns', 5)


User_id = "Santheep Sritharan"
token = "BQCu6GEchEd6XeKUByvh2ZsrZ_jZqICGQeWG6LM-hmsAnPn5fRYheYpwyxHiE8sdsSqhqPoL2aJQYH_VThMi3N02OUfqQ0XtW3znFGYakp4ulAW8hknVf3AIqVa-xVg7wzh1gAVtyF7G-GeICb-SvX7EorZdGHnrezhvk2gv"

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

    try:
        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
            time=yesterday_unix_timestamp), headers = headers)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print("Invalid spotify token error:", str(r))
        raise SystemExit(err)

    data = r.json()
    print("Data retrieved")

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

#Run data validation
if validate_data(song_df):
    print("Data valid")

# Load data to the database
# engine = sqlalchemy.create_engine(Mysql_url)    #Create a new engine instance
# conn = sql
#
# def CONNECT_TO_DB():
#     try:
#         cnx = mysql.connector.connect(user='root', password='xxxxx',
#                               host='localhost',
#                               database='spotify')
#     except mysql.connector.Error as err:
#         if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#             print("Something is wrong with your user name or password")
#         elif err.errno == errorcode.ER_BAD_DB_ERROR:
#             print("Database does not exist")
#         else:
#             print(err)
#     else:
#         cnx.close()


TABLES = {}
TABLES['spotify_playlist'] = (
    "CREATE TABLE 'spotify_playlist' ("
    " 'song_name' VARCHAR(200),"
    " 'artist_name' VARCHAR(200),"
    " 'release_date' DATE,"
    " 'played_at' VARCHAR(200),"
    " 'timestamp' VARCHAR(200),"
    " CONSTRAINT primary_key_constraint PRIMARY KEY ('played_at')"
    ") ENGINE=InnoDB")

cnx = mysql.connector.connect(user='root', password='xxxxx',
                              host='localhost',
                              database='spotify')

cursor = cnx.cursor()

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host="localhost", db="spotify", user="root", pw="xxxx"))

try:
    song_df.to_sql("spotify_playlist", engine, index=False, if_exists='append')
except:
    print("Data already exists in the database")

#Make sure data is committed to the database
cnx.commit()

cursor.close()
cnx.close()

print(song_df)

