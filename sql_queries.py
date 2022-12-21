import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INTEGER,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id INTEGER,
        location TEXT,
        user_agent TEXT,
        FOREIGN KEY (start_time) REFERENCES time(start_time),
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level TEXT
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        artist_id TEXT,
        year INTEGER,
        duration FLOAT NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION {}
""").format()

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION {}
""").format()

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TO_TIMESTAMP(ts::double precision / 1000) AS start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent,
    FROM staging_events AS se
    LEFT JOIN stagint_songs AS ss ON se.song = ss.title
                                 AND se.artist = ss.artist_name
                                 AND se.length = ss.duration
""")

user_table_insert = ("""
    WITH user_more_recent_info AS (
        SELECT user_id,
               MAX(ts) AS more_recent_ts
        FROM staging_events
        WHERE page = 'NextSong'
        GROUP BY user_id
    )
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT 
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events AS AS se
    INNER JOIN user_more_recent_info AS umri ON se.ts = umri.more_recent_ts
                                            AND se.user_id = umri.user_id
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        TO_TIMESTAMP(ts::double precision / 1000) AS start_time,
        DATE_PART('hour', TO_TIMESTAMP(ts::double precision / 1000)) AS hour,
        DATE_PART('day', TO_TIMESTAMP(ts::double precision / 1000)) AS day,
        DATE_PART('week', TO_TIMESTAMP(ts::double precision / 1000)) AS week,
        DATE_PART('month', TO_TIMESTAMP(ts::double precision / 1000)) AS month,
        DATE_PART('year', TO_TIMESTAMP(ts::double precision / 1000)) AS year,
        DATE_PART('dow', TO_TIMESTAMP(ts::double precision / 1000)) AS weekday
    FROM staging_events
    WHERE page = 'NextSongs'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
