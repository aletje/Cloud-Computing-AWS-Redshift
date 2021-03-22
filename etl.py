import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Description: 
        - Copies the user log data and song meta data from JSON files stored in S3 bucket
        into the staging tables stg_events, stg_songs in Redshift

        Arguments:
        - Parameter cur: the cursor object
        - Parameter conn: connection to sparkifydb

        Returns:
        - None
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Issue loading data to staging")
            print (e)
        try:
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue committing to DB")
            print (e)

            
def insert_tables(cur, conn):
    """
        Description: 
        - Extract relevant subset of the staging tables stg_events, stg_songs
        - Populate the fact and dimension tables
        
        Arguments:
        - Parameter cur: the cursor object
        - Parameter conn: connection to sparkifydb
        
        Returns:
        - None
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Issue inserting into table")
            print (e)
        try:
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue committing to DB")
            print (e)


def main():
    """
        Description:
        - Parse and read our Redshift DB configs in dwh.cfg file 
        - Open connection to sparkifydb
        - Create cursor
        - Call the load_staging_tables function
        - Call the insert_tables function
        - Close connection to DB

        Arguments:
        - load_staging_tables(cur): cursor object
        - load_staging_tables(conn): connection to db
        - insert_tables(cur): cursor object
        - insert_tables(conn): connection to db

        Returns:
        - None
    """
    config = configparser.ConfigParser()
    
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()