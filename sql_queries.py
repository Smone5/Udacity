import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

iam_role = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "event_stage" (
  "artist" varchar,
  "auth" varchar,
  "firstName" varchar,
  "gender" varchar(4),
  "itemInSession" varchar,
  "lastName" varchar,
  "length" varchar,
  "level" varchar,
  "location" varchar,
  "method" varchar,
  "page" varchar,
  "registration" varchar,
  "sessionId" varchar,
  "song" varchar,
  "status" varchar,
  "ts" varchar,
  "userAgent" varchar,
  "userId" varchar
);
""")

staging_songs_table_create = ("""
CREATE TABLE "song_stage" (
  "artist_id" varchar PRIMARY KEY,
  "artist_latitude" varchar,
  "artist_location" varchar,
  "artist_longitude" varchar,
  "artist_name" varchar,
  "duration" varchar,
  "num_songs" varchar,
  "song_id" varchar,
  "title" varchar,
  "year" varchar
);
""")

songplay_table_create = ("""
CREATE TABLE "fact_songplay" (
  "songplay_id" varchar PRIMARY KEY,
  "start_time" bigint NOT NULL,
  "user_id" int NOT NULL,
  "level" varchar NOT NULL,
  "song_id" varchar,
  "artist_id" varchar,
  "session_id" int,
  "location" varchar,
  "user_agent" varchar
);
""")

user_table_create = ("""
CREATE TABLE "dim_user" (
  "user_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar,
  "last_name" varchar,
  "gender" varchar,
  "level" varchar NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE "dim_song" (
  "song_id" varchar PRIMARY KEY NOT NULL,
  "title" varchar NOT NULL,
  "artist_id" varchar,
  "year" int,
  "duration" float8
);
""")

artist_table_create = ("""
CREATE TABLE "dim_artist" (
  "artist_id" varchar PRIMARY KEY NOT NULL,
  "name" varchar NOT NULL,
  "location" varchar,
  "latitude" float8,
  "longitude" float8
);
""")

time_table_create = ("""
CREATE TABLE "dim_time" (
  "time_key" bigint PRIMARY KEY NOT NULL,  
  "start_time" timestamp NOT NULL,
  "hour" int NOT NULL,
  "day" int NOT NULL,
  "week" int NOT NULL,
  "month" int NOT NULL,
  "year" int NOT NULL,
  "weekday" varchar NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy event_stage
from 's3://udacity-dend/log_data/2018/11/2018'
credentials 'aws_iam_role={}'
format as json 's3://udacity-dend/log_json_path.json';
""").format(iam_role)



staging_songs_copy = ("""
copy event_stage
from 's3://udacity-dend/log_data/2018/11/2018'
credentials 'aws_iam_role={}'
format as json 's3://udacity-dend/log_json_path.json';
""").format(iam_role)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    e.userId || st.song_id || e.itemInSession as songplay_id,
    CAST(e.ts as bigint) as start_time,
    CAST(e.userId as int) as user_id,
    e.level as level,
    st.song_id,
    st.artist_id,
    CAST(e.itemInSession as int) as session_id,
    e.location as location,
    e.userAgent as user_agent
FROM (select * from event_stage where page = 'NextSong') as e
LEFT JOIN song_stage st
    ON (e.artist = st.artist_name OR e.song = st.title)
WHERE song_id <> 'None'
ORDER BY start_time ASC
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    CAST(e.userID as integer) AS user_id,
    e.firstName AS first_name,
    e.lastName AS last_name,
    e.gender AS gender,
    e.level AS level
FROM event_stage e
WHERE e.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    s.song_id AS song_id,
    s.title AS title,
    s.artist_id AS artist_id,
    CAST(s.year as integer) AS year,
    CAST(s.duration as decimal(8,2)) AS duration
FROM song_stage s
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    s.artist_id AS artist_id,
    s.artist_name AS name,
    s.artist_location AS location,
    CONVERT(float, s.artist_latitude) AS latitude,
    CONVERT(float, s.artist_longitude) AS longitude
FROM song_stage s
JOIN event_stage e
    ON (e.artist = s.artist_name AND e.song = s.title)
WHERE e.page = 'NextSong'
""")

time_table_insert = ("""
INSERT INTO dim_time(time_key, start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    CAST(e.ts as bigint) AS time_key,
    TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
    EXTRACT(hour from TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second') AS hour,
    CAST(DATE_PART(day, TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second')  as Integer) AS day,
    CAST(DATE_PART(week, TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second') as Integer) AS week,
    CAST(DATE_PART(month, TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second') as Integer) AS month,
    CAST(DATE_PART(year, TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second') as Integer) AS year,
    CASE
        WHEN(
                DATE_PART(dayofweek, TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second') = 0.0
                OR
                DATE_PART(dayofweek, TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second') = 6.0
            )
        THEN 'no'
        ELSE 'yes'
        END
        AS weekday
FROM event_stage e
WHERE e.page = 'NextSong'
ORDER BY time_key ASC;

""")


# Duplication Check Queries

table_names = ['dim_user', 'dim_song', 'dim_artist', 'dim_time']
user_table_dups = ('''
SELECT user_id, first_name, last_name, level, COUNT(*)
FROM sparkify.dim_user
GROUP BY user_id, first_name, last_name, level
ORDER BY COUNT(*) DESC
LIMIT 5
;
''')
song_table_dups = ('''

''')
artist_table_dups =('''

''')
time_table_dups = ('''

''')



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
