"""
SQL Queries for the Redshift cluster DB, structured following the star-schema:

#FACT TABLE:
songplays - records in log data associated with song plays

#DIMENSION TABLES:
users - users in the app
songs - songs in music database
artists - artists in music database
times - timestamps of records in songplays broken down into specific units
"""

import json

# Constants
CFG_FILE = 'dwh_config.json'

# Load configuration from JSON file
with open(CFG_FILE) as f:
    config = json.load(f)

# DROP TABLE QUERIES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLE QUERIES

# Staging tables
staging_events_table_create = """
CREATE TABLE staging_events (
    ...
);
"""

staging_songs_table_create = """
CREATE TABLE staging_songs (
    ...
);
"""

# Fact table
songplay_table_create = """
CREATE TABLE songplays (
    ...
);
"""

# Dimension tables
user_table_create = """
CREATE TABLE users (
    ...
) diststyle all;
"""

song_table_create = """
CREATE TABLE songs (
    ...
) diststyle all;
"""

artist_table_create = """
CREATE TABLE artists (
    ...
) diststyle all;
"""

time_table_create = """
CREATE TABLE times (
    ...
) diststyle all;
"""

# COPY DATA INTO STAGING TABLES FROM S3
staging_events_copy = """
COPY staging_events FROM '{}' 
...
""".format(config['S3']['LOG_DATA'], ...)

staging_songs_copy = """
COPY staging_songs FROM '{}'
...
""".format(config['S3']['SONG_DATA'], ...)

# INSERT DATA INTO FACT AND DIMENSION TABLES FROM STAGING TABLES

songplay_table_insert = """
INSERT INTO songplays (
    ...
);
"""

user_table_insert = """
INSERT INTO users(
    ...
);
"""

song_table_insert = """
INSERT INTO songs(
    ...
);
"""

artist_table_insert = """
INSERT INTO artists(
    ...
);
"""

time_table_insert = """
INSERT INTO times(
    ...
);
"""

# QUERY LISTS FOR EASY REFERENCE

# These lists are used in the main ETL script to iterate over and execute each query
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]