import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
     load staging tables of sparkify database 
     
     parameter:
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
    return :no
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
     insert into sparkify star schema from staging tables
    
    parameter:
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
    return :no
     '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    main fuction of etl script
    loads AWS parameter from config file
    load staging tables of sparkify database 
    insert data into sparkify star schema from staging tables
    
    parameter:no
       
    return :no
     '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()