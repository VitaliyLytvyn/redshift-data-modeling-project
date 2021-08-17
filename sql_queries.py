import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
     artist varchar,
     auth varchar,
     firstName varchar,
     gender varchar,
     itemInSession int,
     lastName varchar,
     length double precision,
     level varchar,
     location varchar,
     method varchar,
     page varchar,
     registration double precision,
     sessionId int,
     song varchar,
     status int,
     ts bigint,
     userAgent varchar,
     userId int)
"""

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int NOT NULL,
    artist_id varchar,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int)
""")

#fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int IDENTITY(0,1) PRIMARY KEY NOT NULL,
    start_time timestamp NOT NULL,
    userId int NOT NULL   distkey,
    level varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    sessionId int NOT NULL,
    location varchar NOT NULL,
    userAgent varchar NOT NULL)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    userId int PRIMARY KEY NOT NULL  sortkey distkey,
    firstName varchar NOT NULL,
    lastName varchar NOT NULL,
    gender char(1) NOT NULL,
    level varchar NOT NULL)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY  sortkey,
    title varchar NOT NULL,
    artist_id varchar,
    year int NOT NULL,
    duration float NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY  sortkey,
    artist_name varchar NOT NULL,
    artist_location varchar NOT NULL,
    artist_latitude double precision,
    artist_longitude double precision
    )diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp PRIMARY KEY NOT NULL  sortkey,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
    )diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
json {}
region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
iam_role {}
json 'auto' 
region 'us-west-2'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, userId, level, song_id, artist_id,
                           sessionId, location, userAgent)
SELECT DISTINCT timestamp 'epoch' + se.ts * interval '0.001 seconds'
       as start_time,
       se.userId,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionId,
       se.location,
       se.userAgent
FROM staging_events AS se
LEFT JOIN staging_songs AS ss
ON se.artist = ss.artist_name
    AND se.song = ss.title
    AND se.length = ss.duration
WHERE se.page = 'NextSong' 
    AND se.userId IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (userId, firstName, lastName, gender, level)
SELECT DISTINCT userId,
       firstName,
       lastName,
       gender,
       level
FROM staging_events
WHERE page='NextSong'
    AND userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location,
                        artist_latitude, artist_longitude)
SELECT DISTINCT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM staging_songs
WHERE artist_location IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'
       as start_time,
EXTRACT (HOUR FROM start_time) AS hour,
EXTRACT (DAY FROM start_time) AS day,
EXTRACT (WEEKS FROM start_time) AS week,
EXTRACT (MONTH FROM start_time) AS month,
EXTRACT (YEAR FROM start_time) AS year,
EXTRACT (WEEKDAY FROM start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
