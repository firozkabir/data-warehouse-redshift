import configparser



# Try to load values from config file
# values are used to populate some of the sql strings
try:
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    s3_bucket_log = config['S3']['LOG_DATA']
    s3_bucket_song = config['S3']['SONG_DATA']
    log_json_path = config['S3']['LOG_JSONPATH']
    iam_role_arn = config['IAM_ROLE']['ARN']
except Exception as e:
    print(f"ERROR while trying to read config file dwh.cfg: {e}")
    raise e

# DROP TABLES

s_events_table_drop = "drop table if exists s_events;"
s_songs_table_drop = "drop table if exists s_songs;"
f_songplay_table_drop = "drop table if exists f_songplay;"
d_user_table_drop = "drop table if exists d_user;"
d_song_table_drop = "drop table if exists d_song;"
d_artist_table_drop = "drop table if exists d_artists;"
d_time_table_drop = "drop table if exists d_time;"

# CREATE TABLES

s_events_table_create= ("""
    create table if not exists s_events
    (
        artist          varchar,
        auth            varchar,
        firstName       varchar,
        gender          varchar,
        itemInSession   integer,
        lastName        varchar,
        length          float,
        level           varchar,
        location        varchar,
        method          varchar,
        page            varchar,
        registration    float,
        sessionId       integer,
        song            varchar,
        status          integer,
        ts              timestamp,
        userAgent       varchar,
        userId          integer
    );
""")

s_songs_table_create = ("""
    create table if not exists s_songs
    (
        song_id              varchar,
        title                varchar,
        duration             float,
        year                 int,
        num_songs            int,
        artist_id            varchar,
        artist_latitude      float,
        artist_longitude     float,
        artist_location      varchar,
        artist_name          varchar
    );
""")

f_songplay_table_create = ("""
    create table if not exists f_songplays
    (
        songplay_id     integer         identity(0,1)   primary key, 
        start_time      timestamp       not null        sortkey distkey, 
        user_id         integer         not null, 
        level           varchar         not null, 
        song_id         varchar         not null, 
        artist_id       varchar         not null, 
        session_id      integer                 , 
        location        varchar                 , 
        user_agent      varchar
    );

""")

d_user_table_create = ("""
    create table if not exists d_users 
    (
        user_id         integer         primary key, 
        first_name      varchar         not null, 
        last_name       varchar         not null, 
        gender          varchar         not null, 
        level           varchar                  sortkey
    );
""")

d_song_table_create = ("""
    create table if not exists d_songs
    (
        song_id         varchar         primary key, 
        title           varchar         not null sortkey, 
        artist_id       varchar         not null, 
        year            integer         not null, 
        duration        float
    );
""")

d_artist_table_create = ("""
    create table if not exists d_artists
    (
        artist_id       varchar         primary key, 
        name            varchar         not null sortkey, 
        location        varchar                 , 
        latitude        float                   , 
        longitude       float                   
    );
""")

d_time_table_create = ("""
    create table if not exists d_time
    (
        start_time      timestamp       primary key sortkey distkey, 
        hour            integer         not null, 
        day             integer         not null, 
        week            integer         not null, 
        month           integer         not null, 
        year            integer         not null, 
        weekday         varchar         not null
    );
""")

# STAGING TABLES

s_events_copy = (f"""
    copy s_events from {s3_bucket_log}
    credentials 'aws_iam_role={iam_role_arn}'
    region 'us-west-2' 
    format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""")

s_songs_copy = (f"""
    copy s_songs from {s3_bucket_song}
    credentials 'aws_iam_role={iam_role_arn}'
    region 'us-west-2' 
    format as JSON 'auto';
""")

# FINAL TABLES

f_songplay_table_insert = ("""
    insert into f_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select  distinct(e.ts)  as start_time, 
            e.userId        as user_id, 
            e.level         as level, 
            s.song_id       as song_id, 
            s.artist_id     as artist_id, 
            e.sessionId     as session_id, 
            e.location      as location, 
            e.userAgent     as user_agent
    from s_events e
    join s_songs  s   on (e.song = s.title and e.artist = s.artist_name)
    and e.page  =  'NextSong'
""")

d_user_table_insert = ("""
    insert into d_users (user_id, first_name, last_name, gender, level)
    select  distinct(userId)    as user_id,
             firstName           as first_name,
             lastName            as last_name,
             gender,
             level
    from s_events
    where user_id is not null
    and page  =  'NextSong';
""")

d_song_table_insert = ("""
    insert into d_songs (song_id, title, artist_id, year, duration)
    select  distinct(song_id) as song_id,
            title,
            artist_id,
            year,
            duration
    from s_songs
    where song_id is not null;
""")

d_artist_table_insert = ("""
    insert into d_artists (artist_id, name, location, latitude, longitude)
    select  distinct(artist_id) as artist_id,
            artist_name         as name,
            artist_location     as location,
            artist_latitude     as latitude,
            artist_longitude    as longitude
    from s_songs
    where artist_id is not null;    
""")

d_time_table_insert = ("""
    insert into d_time (start_time, hour, day, week, month, year, weekday)
    select  distinct(start_time)                as start_time,
            extract(hour FROM start_time)       as hour,
            extract(day FROM start_time)        as day,
            extract(week FROM start_time)       as week,
            extract(month FROM start_time)      as month,
            extract(year FROM start_time)       as year,
            extract(dayofweek FROM start_time)  as weekday
    from f_songplays;
""")

# QUERY LISTS
# these are now dictionaries instead of list to allow more feedback 
# in particular, we can now tell which queiry is running and 
# in case of a bug, narrow our investigation to that sql string

create_table_queries = {
    's_events_table_create': s_events_table_create,
    's_songs_table_create': s_songs_table_create, 
    'f_songplay_table_create': f_songplay_table_create, 
    'd_user_table_create': d_user_table_create, 
    'd_song_table_create': d_song_table_create, 
    'd_artist_table_create': d_artist_table_create, 
    'd_time_table_create': d_time_table_create
}

drop_table_queries = {
    's_events_table_drop': s_events_table_drop, 
    's_songs_table_drop': s_songs_table_drop, 
    'f_songplay_table_drop': f_songplay_table_drop, 
    'd_user_table_drop': d_user_table_drop, 
    'd_song_table_drop': d_song_table_drop, 
    'd_artist_table_drop': d_artist_table_drop, 
    'd_time_table_drop': d_time_table_drop
}

copy_table_queries = {
    's_events_copy': s_events_copy, 
    's_songs_copy': s_songs_copy
}


insert_table_queries = {
    'f_songplay_table_insert': f_songplay_table_insert, 
    'd_user_table_insert': d_user_table_insert, 
    'd_song_table_insert': d_song_table_insert, 
    'd_artist_table_insert': d_artist_table_insert, 
    'd_time_table_insert': d_time_table_insert
}
