CREATE stage staging
URL = "s3://spotify-etl-project-qjf/transformed_data/"
CREDENTIALS = (AWS_KEY_ID = 'KEY_ID' AWS_SECRET_KEY = 'SECRET_KEY');

CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
PARSE_HEADER = true;

SELECT *
FROM TABLE(INFER_SCHEMA(
    LOCATION=>'@staging/album/',
    FILE_FORMAT=>'csv_file_format'
))

CREATE OR REPLACE TABLE album USING TEMPLATE (
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
WITHIN GROUP (ORDER BY ORDER_ID)
FROM TABLE (INFER_SCHEMA(
LOCATION=>'@staging/album/',
FILE_FORMAT=>'csv_file_format')));

COPY INTO album
FROM @staging/album/
file_format = (skip_header = 1);

SELECT *
FROM TABLE(INFER_SCHEMA(
    LOCATION=>'@staging/artist/',
    FILE_FORMAT=>'csv_file_format'
))

CREATE OR REPLACE TABLE artist USING TEMPLATE (
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
WITHIN GROUP (ORDER BY ORDER_ID)
FROM TABLE (INFER_SCHEMA(
LOCATION=>'@staging/artist/',
FILE_FORMAT=>'csv_file_format')));

COPY INTO artist
FROM @staging/artist/
file_format = (skip_header = 1);

SELECT *
FROM TABLE(INFER_SCHEMA(
    LOCATION=>'@staging/song/',
    FILE_FORMAT=>'csv_file_format'
))

CREATE OR REPLACE TABLE song USING TEMPLATE (
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
WITHIN GROUP (ORDER BY ORDER_ID)
FROM TABLE (INFER_SCHEMA(
LOCATION=>'@staging/song/',
FILE_FORMAT=>'csv_file_format')));

COPY INTO song
FROM @staging/song/
file_format = (skip_header = 1);

SELECT * FROM album;
SELECT * FROM artist;
SELECT * FROM song;
