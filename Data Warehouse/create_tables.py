import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''Drop tables of sparkify database
    
       parameters :
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
       return : no return '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''Create tables of sparkify database
    
       parameters :
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
       return : no return '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    main fuction of create tables script.
    load AWS parameter from config file
    Connects to database and execute creation script.
    
    parameter :no
    
    return : no    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()