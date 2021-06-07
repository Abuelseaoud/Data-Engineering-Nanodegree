# Project 1: Data modeling with Postgres

This **Udacity Data Engineering nanodegree** project creates a postgres database `sparkifydb` for a music app, *Sparkify*.
The purpose of the database is to model song and log datasets (originaly stored in JSON format) with a star schema optimised for queries on song play analysis.

## Schema design and ETL pipeline
he star schema has 1 *fact* table (songplays), and 4 *dimension* tables (users, songs, artists, time).
`DROP`, `CREATE`, `INSERT`, and `SELECT` queries are defined in **sql_queries.py**. **create_tables.py** 
uses functions `create_database`, `drop_tables`, and `create_tables` to create the database sparkifydb and the required tables.
![Schema ERD](Song_ERD.png)

Extract, transform, load processes in **etl.py** populate the **songs** and **artists** tables with data derived from the JSON song files,
`data/song_data`. Processed data derived from the JSON log files, `data/log_data`, is used to populate **time** and **users** tables.
A `SELECT` query collects song and artist id from the **songs** and **artists** tables and 
combines this with log file derived data to populate the **songplays** fact table.

## Project Repository files
*Python files  *py* :
    1.sql_queries.py   : python file have all creation / insertion queries for creating tables in database and insert data from files
    1.create_tables.py : python script drops all tables if exist and recreate it
    1.etl.py           : python script preform ETL operation(extract from json file ,transafer data to proper format ,load data in tables) 
 for all files in data.zip
 
 * Python notebook files *ipynb* :
    1. etl.ipynb  : demonstrates ETL process step by step to check data extraction and format changing
    1. Run.ipynb  : to execute project by run create_tables.py then etl.py  
    1. test.ipynb : to test the content of database tables 
          
* Other files:
   1. data.zip     : data compressed file for(log_files&song_files)
   1. Song_ERD.png : ERD Diagram of  `sparkifydb` database 
   1. dend-p1-lessons-cheat-sheet.pdf : very helpful cheat sheet to recap most of project functions and modules
 
 ## How To Run the Project
  * Way 1 : to run all cells in *Run.ipynb*
  * Way 2 : to execute the following python commands from terminal
       1. python3 create_tables.py
       1. python3 etl.py
