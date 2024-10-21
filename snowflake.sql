-- Create a new database 
CREATE DATABASE DE_PROJECT;

-- Switch to the newly created database
USE DATABASE DE_PROJECT;

-- Create table to load CSV data
CREATE or replace TABLE weather_data(
    temp       NUMBER(20,0),
    CITY          VARCHAR(128) 
    ,humidity   NUMBER(20,5)
    ,wind_speed      NUMBER(20,5) 
   ,time             VARCHAR(128)  
   ,wind_dir        VARCHAR(128)
   ,pressure_mb    NUMBER(20,5)
);


--Create integration object for external stage
create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::381491882wer:role/AWS2Snowflake'
  storage_allowed_locations = ('s3://de-project-datewithdata/snowflake/');

  
--Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION s3_int;

create or replace file format csv_format
                    type = csv
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;
                    
create or replace stage ext_csv_stage
  URL = 's3://de-project-datewithdata/snowflake/'
  STORAGE_INTEGRATION = s3_int
  file_format = csv_format;

--create pipe to automate data ingestion from s3 to snowflake
create or replace pipe mypipe auto_ingest=true as
copy into weather_data
from @ext_csv_stage
on_error = CONTINUE;

show pipes;

select * from weather_data;