# Import necessary libraries
import configparser  # Read and write data from and to INI file.
import psycopg2  # Run SQL commands in a PostgreSQL database.
from sql_queries import copy_table_queries, insert_table_queries  # Import queries to load and transform data from `sql_queries.py`.

def load_staging_tables(cur, conn):
    """
    Loads data from files stored in S3 to the staging tables using the queries in `copy_table_queries` list.
    cur : cursor object. Allows Python code to execute PostgreSQL command in a database session.
    conn : connection object. Handles the connection to a PostgreSQL database instance.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Selects and transforms data from staging tables into the dimensional tables using the queries in `insert_table_queries` list.
    cur : cursor object. Allows Python code to execute PostgreSQL command in a database session.
    conn : connection object. Handles the connection to a PostgreSQL database instance.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Extracts songs metadata and user activity data from S3,
    - Transforms it using a staging table, and loads it into dimensional tables for analysis.
    - Establishes connection with the database and gets cursor to it,
    - Loads staging tables,
    - Inserts data into dimension tables.
    - Finally, closes the connection.
    """
    # Read database connection params
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to PostgreSQL database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load staging tables and insert data into dimension tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close database connection
    conn.close()


if __name__ == "__main__":
    main()