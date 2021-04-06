# Summary 
### Sample project that illustrates the use of Cloud Datawarehousing on AWS
- Launched a new Redshift cluster using the Python Infrastructure as Code (IaC) boto3 library
- Created a new IAM role with read access to S3
- Created 2 staging tables in Redshift Postgres database
- Created 1 fact and 4 dimension tables in Redshift Postgres database
- Created an ETL pipeline that 
    - transfers data from S3 into the staging tables in Redshift
    - populates the fact and dimension tables
- Provided 1 example query

#### Running the script

##### ETL-job
- Open a new Ipython notebook and run the commands in the following order
    - `%run create_tables.py`
    - `%run etl.py`

# sparkifydb
#### Purpose
This database is designed for analysing song plays from Sparkify app

#### Database schema design
- Staging tables
    - `stg_events` loads user log data from S3 into Redshift
    - `stg_songs` loads metadata from S3 into Redshift
- Fact table 
    - `songplays` are records in log data associated with song plays i.e. records with page NextSong
- Dimension tables
    - `users` are users in the app
    - `songs`  are songs from music metadata database
    - `artists` are artists from music metadata database
    - `time` are timestamps of records in songplays broken down into specific units
    
#### ETL pipeline
The ETL pipeline `etl.py` uses Python and Postgres SQL to transfer data from S3:
- Song JSON metadata files in `s3://udacity-dend/song_data` populates `stg_songs`
- Log JSON files in `s3://udacity-dend/log_data` populates `stg_events`
- A subset from `stg_songs` and `stg_events` populates the fact and dimension tables using SQL statements, here's an example:
`INSERT INTO songplays (...) 
SELECT col1, col2,..
FROM stg_events e
JOIN stg_songs s ON s.artist_name = e.artist
WHERE e.page = 'NextSong'`

#### Example - analytical query
##### Count users and their level for the service
`SELECT count(d.user_id) as cnt_user, d.level
 FROM songplays f 
 INNER JOIN users d ON d.user_id = f.user_id
 GROUP BY d.level`