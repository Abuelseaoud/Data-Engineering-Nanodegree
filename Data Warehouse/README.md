# Project Data warehouse

<p align="center"><img src="https://github.com/Abuelseaoud/Data-Engineering-Nanodegree/blob/main/Data%20Warehouse/logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

## Project description

Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## How to run

1. To run this project you will need to fill the following information, and save it as *dwh.cfg* in the project root folder.

```
[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[CLUSTER_CONFIG]
dwh_cluster_identifier = dwhCluster
dwh_cluster_type = multi-node
dwh_num_nodes = 4
dwh_node_type = dc2.large

[AWS]
key =
secret = 
region = us-east-1


[IAM_ROLE]
dwh_iam_role_name = 
arn = 

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'


```


3. Run the *IAC_START* script to set up the needed infrastructure for this project.

    `$ python3 IAC_start.py`

4. Run the *create_tables* script to set up the database staging and analytical tables

    `$ python4 create_tables.py`

5. Finally, run the *etl* script to extract data from the files in S3, stage it in redshift, and finally store it in the dimensional tables.

    `$ python3 etl.py`
    
6. optional :when you need to delete your cluster run *IAC_shutdown* script .it deletes the cluster and the arn.

    `$ python3 IAC_shutdown.py`




## Project structure

This project includes five script files:

- IAC_start.py is where the AWS components for this project are created programmatically
- IAC_shutdown.py is where the AWS components for this project are deleted programmatically
- create_table.py is where fact and dimension tables for the star schema in Redshift are created.
- etl.py is where data gets loaded from S3 into staging tables on Redshift and then processed into the analytics tables on Redshift.
- sql_queries.py where SQL statements are defined, which are then used by etl.py, create_table.py and analytics.py.
- README.md is current file.

## Database schema design

<p align="center"><img src="https://github.com/Abuelseaoud/Data-Engineering-Nanodegree/blob/main/Data%20Warehouse/schema%20design.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

#### Staging Tables
- staging_events
- staging_songs

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong .

     _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

#### Dimension Tables
- users - users in the app .

     _user_id, first_name, last_name, gender, level_
  
  
- songs - songs in music database .

     _song_id, title, artist_id, year, duration_
  
  
- artists - artists in music database .

     _artist_id, name, location, lattitude, longitude_
  
  
- time - timestamps of records in songplays broken down into specific units .

     _start_time, hour, day, week, month, year, weekday_




### Steps followed on this project

#### 1. Create Table Schemas
- Design schemas for your fact and dimension tables
- Write a SQL CREATE statement for each of these tables in sql_queries.py
- Complete the logic in create_tables.py to connect to the database and create these tables
- Write SQL DROP statements to drop tables in the beginning of - create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
- Launch a redshift cluster and create an IAM role that has read access to S3.
- Add redshift database and IAM role info to dwh.cfg.
- Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

#### 2. Build ETL Pipeline
- Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
- Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
- Delete your redshift cluster when finished.

