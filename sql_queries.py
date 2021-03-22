import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stg_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR(1),
        item_in_session SMALLINT,
        last_name VARCHAR,
        length NUMERIC,
        level CHAR(4),
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        session_id BIGINT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stg_songs (
    num_songs SMALLINT,
    artist_id VARCHAR NOT NULL,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    duration NUMERIC,
    year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint IDENTITY(1,1) PRIMARY KEY,
        start_time  bigint NOT NULL,
        user_id     int NOT NULL sortkey,
        level       varchar,
        song_id     varchar distkey,
        artist_id   varchar,
        session_id  int,
        location    varchar,
        user_agent  varchar
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    int PRIMARY KEY sortkey,
        first_name varchar,
        last_name  varchar,
        gender     char(1),
        level      varchar
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   varchar PRIMARY KEY sortkey distkey,
        title     varchar,
        artist_id varchar,
        year      int,
        duration  numeric
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY sortkey,
        name      varchar,
        location  varchar,
        latitude  numeric,
        longitude numeric
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY sortkey,
        hour       int,
        day        int,
        week       int,
        month      int,
        year       int,
        weekday    varchar
        );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stg_events FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 's3://udacity-dend/log_json_path.json'
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
    COPY stg_songs FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (session_id, user_id, artist_id, song_id, start_time, level, location, user_agent)
    SELECT distinct e.session_id, e.user_id, s.artist_id, s.song_id, e.ts, e.level, e.location, e.user_agent
    FROM stg_events e
    JOIN stg_songs s ON s.artist_name = e.artist
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT distinct e.user_id, e.first_name, e.last_name, e.gender, e.level
    FROM stg_events e
    JOIN stg_songs s ON s.artist_name = e.artist
    WHERE e.page = 'NextSong'
      AND e.user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT distinct song_id, title, artist_id, year, duration
    FROM stg_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT distinct artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
    FROM stg_songs
    WHERE artist_id IS NOT NULL
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT distinct
           (timestamp 'epoch' + start_time/1000 * interval '1 second')                       as start_time,
           extract(hour from (timestamp 'epoch' + start_time/1000 * interval '1 second'))    as hour,
           extract(day from (timestamp 'epoch' + start_time/1000 * interval '1 second'))     as day,
           extract(week from (timestamp 'epoch' + start_time/1000 * interval '1 second'))    as week,
           extract(month from (timestamp 'epoch' + start_time/1000 * interval '1 second'))   as month,
           extract(year from (timestamp 'epoch' + start_time/1000 * interval '1 second'))    as year,
           extract(weekday from (timestamp 'epoch' + start_time/1000 * interval '1 second')) as weekday 
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
