# Data warehouse

## 1. dwh.cfg

This is configuration file where most of static data using for project will be defined:

* DWH values to create a cluster.
* CLUSTER values to create database.
* S3 values are paths of JSON files.
* IAM_ROLE is predefined IAM role.


## 2. sql_queries.py

This is where SQL queries will be defined and they will be imported and used by other .py files.

### 2.1. Drop table queries

In here I write queries in order to drop tables if they are existing, otherwise nothing will happen. With `CASCADE` option, the dependencies of table will be dropped as well.

In the below, I design table `songplays` with FOREIGN keys referencing to 4 tables `users, songs, artists` and `time`. Whenever any one of those table dropped, the corresponding FOREIGN KEY will be dropped as well.

```
drop table if exists staging_events cascade;
drop table if exists staging_songs cascade;
drop table if exists songplays cascade;
drop table if exists users cascade;
drop table if exists songs cascade;
drop table if exists artists cascade;
drop table if exists time cascade;
```


### 2.2. Loading settings from dwh.cfg file

We will have some queries to copy data from JSON files into staging table so we will take settings of IAM role and JSON file paths from dwh.cfg file.

```
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH           = config.get("S3", "LOG_JSONPATH")
LOG_DATA               = config.get("S3", "LOG_DATA")
SONG_DATA              = config.get("S3", "SONG_DATA")
```


### 2.3. Create table queries


**staging_event**
```
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
```

This table will have list of columns match key names in the JSON log data. Staging table is just temporary table before data loading to target table. Since I want to load whole data from JSON file into staging table, so I don't put any constraint which could prevent data from being loaded to table.

This table would have large data, that's why I choose EVEN distribution style so data will be divided across the slices almost equally.

I also put columns `page, ts, artist, userId, song` into a cINTERLEAVED SORTKEY since I will use those columns to querying data before load loading to target tables.



**staging_songs**
```
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
```

I create this table with list of columns same with the JSON files of song data. Since this is staging table, I will not put any constraint for this table.

I put distribution style of AUTO for this table so Redshift will decide how to distribute data depend on the size of table.

Also no sort key is defined here since I will select records from this table without any filtering. 



**users**
```
CREATE TABLE IF NOT EXISTS users (
        user_id       CHAR(18) NOT NULL PRIMARY KEY DISTKEY, 
        first_name    NVARCHAR(256), 
        last_name     NVARCHAR(256), 
        gender        VARCHAR(1), 
        level         VARCHAR(4),
        UNIQUE(user_id)) 
DISTSTYLE KEY
COMPOUND SORTKEY(gender, level);
```

*user_id* is UNIQUE value and is PRIMARY KEY of the table. As planned, this table will join with `songplays` to quering data, so we put it as distribution key.

Columns of `gender, level` will make up a compound sortkey since I plan to use them to filtering data.



**songs**
```
CREATE TABLE IF NOT EXISTS songs (
    song_id           CHAR(18) NOT NULL PRIMARY KEY DISTKEY, 
    title             NVARCHAR(256) NOT NULL, 
    artist_id         CHAR(18) NOT NULL, 
    year              SMALLINT, 
    duration          DECIMAL,
    UNIQUE(song_id)) 
DISTSTYLE KEY
SORTKEY(year);
```

*song_id* is UNIQUE value as well as PRIMARY KEY of this table. This column will be used to join with `songplays` table so we will put it as distribution key.

Beside of that, we might need to looking for songs by year when they were written so that I put `year` column as sort key.


**artists**
```
CREATE TABLE IF NOT EXISTS artists (
        artist_id         CHAR(18) NOT NULL PRIMARY KEY DISTKEY, 
        name              NVARCHAR(256) NOT NULL, 
        location          NVARCHAR(256), 
        latitude          DECIMAL, 
        longitude         DECIMAL,
        UNIQUE(artist_id)) 
DISTSTYLE KEY
SORTKEY(location);
```

*artist_id* is UNIQUE value as well as PRIMARY KEY of the table. This column will be joined with `songplays` table so we put it as distribution key.

Beside of that, we might need to looking for artists by their `location` so I put correspoonding column as sort key.


**time**
```
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
```

*start_time* is UNIQUE value as well as PRIMARY KEY of the table. This column will be joined with `songplays` table and so we put it as distribution key.


**songplays**
```
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
```

*songplay_id* with IDENTITY type is UNIQUE value and it is PRIMARY KEY of table.
Beside of that, it will have some FOREIGN keys referencing to other dimension tables.
    * *start_time* referencing to `start_time` column of table **time**
    * *user_id* referencing to `user_id` column of table **users**
    * *song_id* referencing to `song_id` column of table **songs**
    * *artist_id* referencing to `artist_id` column of table **artists**

Since this table will have large data so we choose distribution style of EVEN, then the data will be distributed across slices.

We will filter data using columns `start_time, user_id, song_id, artist_id` so I would include them into a compound sort key.


### 2.4. Copying data to staging tables

**Copying log data into staging_events table**

```
staging_events_copy = ("""
copy staging_events (artist, auth, firstName, gender, itemInSession, 
                    lastName, length, level, location, method, page, registration, sessionId,
                    song, status, ts, userAgent, userId)
from '{}'
iam_role '{}'
json '{}'
COMPUPDATE ON;
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)
```

In here I am using JSON path file to copy data, so the order of columns should be matched to the list in JSON path file.

And this is where we use the settings loaded from configuration file: path of log data (`LOG_DATA`), the IAM role (`DWH_ROLE_ARN`) and path of JSONPath file (`LOG_JSONPATH`).

I am using option `COMPUPDATE ON` here in order to encoding data for each column in order to reduce the size of data as well as improve query performance.


**Copying log data into staging_songs table**

```
staging_songs_copy = ("""
copy staging_songs(num_songs, artist_id, artist_latitude, artist_longitude, 
                   artist_location, artist_name, song_id, title, duration, year)
from '{}'
iam_role '{}'
json 'auto'
COMPUPDATE ON;
""").format(SONG_DATA, DWH_ROLE_ARN)
```

In this case, we are using `auto` option to copy data from song JSON files so the list of columns should be match the key names of JSON file but the order does not matter.

Same with above, we turn on encoding option when copying data.


### 2.5. Inserting data into target tables


**Inserting data into users table**
```
insert into users
select userId, firstName, lastName, gender, level
from staging_events
where userId is not null
and firstName is not null 
and lastName is not null;
```

In here, I am trying to extract user information from **staging_events** table then insert into **users** table.

From **staging_events** table, I just focus on records whose `userId` column not null, that's also the reason I put `userId` column as sort key of this table.

**Inserting data into songs table**
```
insert into songs
select song_id, title, artist_id, year, duration 
from staging_songs;
```

In here, I am simply selecting song information from **staging_songs** table then add them into **songs** table.


**Inserting data into artists table**
```
insert into artists
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
from staging_songs;
```

Same with above, I am selecting artist information from **staging_songs** table then add them into **artists** table.


**Inserting data into time table**
```
insert into time
select start_time, 
    extract(h from start_time) as hour, 
    extract(d from start_time) as day, 
    extract(w from start_time) as week, 
    extract(mon from start_time) as month, 
    extract(y from start_time) as year, 
    extract(dow from start_time) as weekday 
from
(select timestamp with time zone 'epoch' + ts * interval '0.001 second' AS start_time 
 from staging_events
where page = 'NextSong' 
and ts is not null);
```

In here, I am trying to extract datetime parts from values of `ts` column of **staging_events** table. `ts` column's values are number of millisecond, so I will use `epoch` option to convert millisecond into timestamp value, then will extract hour/day/week/month/year/day of week from that returned timestamp value as above query.

And of course, I just need to work with records whose `ts` value not null and `page` value of 'Nextsong' as given condition. This is also the reason I include `ts` and `page` columns into the compound sort key of **staging_events** table.


**Inserting data into songplays table**
```
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
```

The data required to load into **songplays** does not only come from **staging_events** table but also **songs** and **artists** tables.

We need to get `song_id` from **songs** and `artist_id` from **artists**, so need to join those 3 tables together as above query did.

### 2.6. Counting number of records for each table

`counting_queries` list consists queries which will count number of records in each table.


## 3. create_tables.py

This module will perform:
* import drop as well as create table queries from `sql_queries.py`
* drop staging as well as target tables if they are existing.
* try to create those table again to ensure we will have them ready before importing data.



## 4. etl.py

This module will perform:
* import `copy_table_queries` & `insert_table_queries` from `sql_queries.py`
* execute `insert_table_queries` to loading data into staging tables (`staging_events` and `staging_songs`)
* execute `insert_table_queries` to copying data from staging table to target ones (`users`, `songs`, `artists` and `time`).
* execute `counting_queries` to count number of records in each table.



## 5. How to run

* First, create Redshift Cluster and ensure it is available before executing following steps.

* Second, run file `create_tables.py` from `command prompt`. This will connect to database `Sparkify` in Redshift cluster with settings defind in configurations file.
If there is no error occurred, we would have 2 staging and 4 target tables created. It is good to move to the second step.


* Thỉd, run `etl.py` from `command prompt`. 
This will first, loading data from JSON files into staging tables.
Then will copy data from staging table into target one. 
If there is no error occurred, the number of records of each table will be printed out on screen.
