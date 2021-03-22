import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e: 
            print("Error: Issue dropping table")
            print (e)
        
        try:
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue commiting to DB")
            print (e)

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e: 
            print("Error: Issue creating table")
            print (e)
        
        try:
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue commiting to DB")
            print (e)


def main():
    """
    Description:
    - Parse and read our Redshift DB configs in dwh.cfg file 
    - Open connection to sparkifydb
    - Create cursor
    - Drop all tables
    - Create all tables
    - Close connection to DB
    
    Arguments:
    - drop_tables(cur): cursor object
    - drop_tables(conn): connection to db
    - create_tables(cur): cursor object
    - process_date(conn): connection to db
    
    Returns:
    - None
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()