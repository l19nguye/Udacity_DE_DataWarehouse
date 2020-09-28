import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")
DWH_ENDPOINT           = config.get("CLUSTER", "HOST")

DWH_ROLE_ARN           = config.get("IAM_ROLE", "ARN")

LOG_JSONPATH           = config.get("S3", "LOG_JSONPATH")
LOG_DATA               = config.get("S3", "LOG_DATA")
SONG_DATA              = config.get("S3", "SONG_DATA")


# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events cascade"
staging_songs_table_drop = "drop table if exists staging_songs cascade"
songplay_table_drop = "drop table if exists songplays cascade"
user_table_drop = "drop table if exists users cascade"
song_table_drop = "drop table if exists songs cascade"
artist_table_drop = "drop table if exists artists cascade"
time_table_drop = "drop table if exists time cascade"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events ( 
    artist           NVARCHAR(256),
    auth             NVARCHAR(10),
    firstName        NVARCHAR(256),
    gender           VARCHAR(1), 
    itemInSession    SMALLINT, 
    lastName         NVARCHAR(256), 
    length           DECIMAL,
    level            VARCHAR(4),
    location         NVARCHAR(256),
    method           VARCHAR(3),
    page             VARCHAR(20),
    registration     DECIMAL,
    sessionId        SMALLINT,
    song             NVARCHAR(256),
    status           VARCHAR(3),
    ts               BIGINT,
    userAgent        NVARCHAR(256),
    userId           VARCHAR(18)
)
DISTSTYLE EVEN
INTERLEAVED SORTKEY(page, ts, artist, userId, song);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs          SMALLINT,
    artist_id          CHAR(18),
    artist_latitude    DECIMAL,
    artist_longitude   DECIMAL, 
    artist_location    NVARCHAR(256), 
    artist_name        NVARCHAR(256), 
    song_id            CHAR(18),
    title              NVARCHAR(256),
    duration           DECIMAL,
    year               SMALLINT
) DISTSTYLE AUTO;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INT IDENTITY (0, 1) NOT NULL PRIMARY KEY, 
        start_time          TIMESTAMP NOT NULL, 
        user_id             CHAR(18) NOT NULL, 
        level               VARCHAR(4), 
        song_id             CHAR(18), 
        artist_id           CHAR(18),
        session_id          SMALLINT, 
        location            NVARCHAR(256), 
        user_agent          NVARCHAR(256),
        UNIQUE(songplay_id),
        CONSTRAINT fk_time FOREIGN KEY(start_time) REFERENCES time(start_time),
        CONSTRAINT fk_user FOREIGN KEY(user_id) REFERENCES users(user_id),
        CONSTRAINT fk_song FOREIGN KEY(song_id) REFERENCES songs(song_id),
        CONSTRAINT fk_artist FOREIGN KEY(artist_id) REFERENCES artists(artist_id)) 
DISTSTYLE EVEN
COMPOUND SORTKEY(start_time, user_id, song_id, artist_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
        user_id       CHAR(18) NOT NULL PRIMARY KEY DISTKEY, 
        first_name    NVARCHAR(256) NOT NULL, 
        last_name     NVARCHAR(256) NOT NULL, 
        gender        VARCHAR(1), 
        level         VARCHAR(4),
        UNIQUE(user_id)) 
DISTSTYLE KEY
COMPOUND SORTKEY(gender, level);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id           CHAR(18) NOT NULL PRIMARY KEY DISTKEY, 
    title             NVARCHAR(256) NOT NULL, 
    artist_id         CHAR(18) NOT NULL, 
    year              SMALLINT, 
    duration          DECIMAL,
    UNIQUE(song_id)) 
DISTSTYLE KEY
SORTKEY(year);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
        artist_id         CHAR(18) NOT NULL PRIMARY KEY DISTKEY, 
        name              NVARCHAR(256) NOT NULL, 
        location          NVARCHAR(256), 
        latitude          DECIMAL, 
        longitude         DECIMAL,
        UNIQUE(artist_id)) 
DISTSTYLE KEY
SORTKEY(location);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
       start_time      TIMESTAMP NOT NULL PRIMARY KEY DISTKEY, 
       hour            SMALLINT NOT NULL, 
       day             SMALLINT NOT NULL, 
       week            SMALLINT NOT NULL, 
       month           SMALLINT NOT NULL, 
       year            SMALLINT NOT NULL, 
       weekday         SMALLINT NOT NULL,
       UNIQUE(start_time)) 
DISTSTYLE KEY
SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events (artist, auth, firstName, gender, itemInSession, 
                    lastName, length, level, location, method, page, registration, sessionId,
                    song, status, ts, userAgent, userId)
from '{}'
iam_role '{}'
json '{}'
COMPUPDATE ON;
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs(num_songs, artist_id, artist_latitude, artist_longitude, 
                   artist_location, artist_name, song_id, title, duration, year)
from '{}'
iam_role '{}'
json 'auto'
COMPUPDATE ON;
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select DISTINCT timestamp with time zone 'epoch' + e.ts * interval '0.001 second' AS start_time, 
    e.userId, e.level, s.song_id, a.artist_id, e.sessionId, e.location, e.userAgent 
from staging_events e, songs s, artists a
where e.song = s.title
and e.length = s.duration
and e.artist = a.name
and e.page = 'NextSong'
and e.ts is not null
and e.song is not null
and e.userId is not null;
""")

user_table_insert = ("""
insert into users
select DISTINCT userId, firstName, lastName, gender, level
from staging_events
where userId is not null
and firstName is not null 
and lastName is not null;
""")

song_table_insert = ("""
insert into songs
select DISTINCT song_id, title, artist_id, year, duration 
from staging_songs;
""")

artist_table_insert = ("""
insert into artists
select DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
from staging_songs;
""")

time_table_insert = ("""
insert into time
select start_time, 
    extract(h from start_time) as hour, 
    extract(d from start_time) as day, 
    extract(w from start_time) as week, 
    extract(mon from start_time) as month, 
    extract(y from start_time) as year, 
    extract(dow from start_time) as weekday 
from
(select DISTINCT timestamp with time zone 'epoch' + ts * interval '0.001 second' AS start_time 
 from staging_events
where page = 'NextSong' 
and ts is not null);
""")

# COUNTING RECORDS QUERIES
count_staging_events = " select count(*) from staging_events"
count_staging_songs = " select count(*) from staging_songs"
count_users = " select count(*) from users"
count_artists = " select count(*) from artists"
count_time = " select count(*) from time"
count_songplays = " select count(*) from songplays"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
counting_queries = [count_staging_events, count_staging_songs, count_users, count_artists, count_time, count_songplays]