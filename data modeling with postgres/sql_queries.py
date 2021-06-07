# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays "
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL,
start_time timestamp,
user_id int ,
level varchar,
song_id varchar ,
artist_id varchar,
session_id int ,
location varchar,
user_agent varchar,
PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int ,
first_name varchar,
last_name varchar,
gender varchar,
level varchar,
PRIMARY KEY (user_id)
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs( song_id varchar ,
title varchar,
artist_id varchar,
year int,
duration decimal,
PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists( artist_id  varchar , 
name varchar, 
location varchar,
latitude varchar,
longitude varchar,
PRIMARY KEY (artist_id))
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp ,
hour int,
day int,
week int,
month int,
year int,
weekday int,
PRIMARY KEY (start_time))
""")

# INSERT RECORDS
#(pd.to_datetime(row['ts'],unit='ms'),row['userId'],row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent']) 
songplay_table_insert = ("""INSERT INTO songplays(
start_time ,
user_id ,
level ,
song_id ,
artist_id ,
session_id  ,
location ,
user_agent ) values (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""INSERT INTO users(user_id ,
first_name ,
last_name ,
gender ,
level ) values (%s,%s,%s,%s,%s)
ON CONFLICT (user_id)
DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs ( song_id,
title ,
artist_id,
year ,
duration ) values (%s,%s,%s,%s,%s)
ON CONFLICT (song_id)
DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists( artist_id   , 
name , 
location ,
latitude ,
longitude) values (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id )
DO NOTHING
""")


time_table_insert = ("""INSERT INTO time(start_time ,
hour ,
day ,
week ,
month ,
year ,
weekday) values (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time )
DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id  from 
songs join artists on songs.artist_id=artists.artist_id
where songs.title= %s and
artists.name= %s and 
songs.duration= %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
