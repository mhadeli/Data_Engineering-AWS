# Import necessary libraries
import configparser  # Read and write data from and to INI file.
import psycopg2  # Run SQL commands in a PostgreSQL database.
from sql_queries import create_table_queries, \
    drop_table_queries  # Import queries to create and drop tables from `sql_queries.py`.


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    cur : cursor object. Allows Python code to execute PostgreSQL command in a database session.
    conn : connection object. Handles the connection to a PostgreSQL database instance.
    """
    print('Dropping tables')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    cur : cursor object. Allows Python code to execute PostgreSQL command in a database session.
    conn : connection object. Handles the connection to a PostgreSQL database instance.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the Sparkify database and gets cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    # Read database connection params
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to PostgreSQL database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to the cluster')

    # Drop and recreate tables
    drop_tables(cur, conn)
    print('Finished dropping tables')
    create_tables(cur, conn)
    print('Finished creating tables')

    # Close database connection
    conn.close()

if __name__ == "__main__":
    main()