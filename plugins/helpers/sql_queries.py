class SqlQueries:
   
    songplay_table_insert = ("""
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) playid,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id AS songid, 
                songs.artist_id AS artistid, 
                events.sessionid, 
                events.location, 
                events.useragent AS user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT  distinct userid
                , firstname AS first_name
                , lastname AS last_name
                , gender
                , level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT  distinct song_id AS songid
                , title
                , artist_id AS artistid
                , year
                , duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artistid, name, location, latitude, longitude)
        SELECT  distinct artist_id AS artistid
                , artist_name AS name
                , artist_location AS location
                , artist_latitude AS latitude
                , artist_longitude AS longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT  start_time
                , extract(hour from start_time) AS hour
                , extract(day from start_time) AS day
                , extract(week from start_time) AS week
                , extract(month from start_time) AS month
                , extract(year from start_time) AS year
                , extract(dayofweek from start_time) AS weekday
        FROM songplays
    """)
    
    truncate_table = ("""
        TRUNCATE {}
    """)
    
    count_null_songs_table = ("""
        SELECT count(*)
        FROM songs
        WHERE songid is null
    """)
    
    count_null_users_table = ("""
        SELECT count(*)
        FROM users
        WHERE userid is null
    """)
    
    count_null_artists_table = ("""
        SELECT count(*)
        FROM artists
        WHERE artistid is null
    """)
    
    count_null_time_table = ("""
        SELECT count(*)
        FROM time
        WHERE start_time is null
    """)
    