import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

pg_host = os.getenv("PGHOST")
pg_user = os.getenv("PGUSER")
pg_port = os.getenv("PGPORT")
pg_database = os.getenv("PGDATABASE")
pg_password = os.getenv("PGPASSWORD")





def create_table():
    try:
        conn = psycopg2.connect(
        host=pg_host,
        user=pg_user,
        port=pg_port,
        database=pg_database,
        password=pg_password
         )


        # Create a cursor object using the connection
        cursor = conn.cursor()

        # Define the create table query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS registered_trips (
            TRIPDURATION INT,
            STARTTIME TIMESTAMP WITH TIME ZONE,
            STOPTIME TIMESTAMP WITH TIME ZONE,
            START_STATION_ID INT,
            START_STATION_NAME TEXT,
            START_STATION_LATITUDE FLOAT,
            START_STATION_LONGITUDE FLOAT,
            END_STATION_ID INT,
            END_STATION_NAME TEXT,
            END_STATION_LATITUDE FLOAT,
            END_STATION_LONGITUDE FLOAT,
            BIKEID INT,
            MEMBERSHIP_TYPE TEXT,
            USERTYPE TEXT,
            BIRTH_YEAR FLOAT,
            GENDER INT,
            REGISTRATION_STATUS BOOLEAN,
            REGISTRATION_DATE DATE
        )
        """

        # Execute the create table query
        cursor.execute(create_table_query)

        # Commit the transaction
        conn.commit()

        print("Table 'registered_trips' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)

def insert_records(table_name, df):
    try:
        conn = psycopg2.connect(
        host=pg_host,
        user=pg_user,
        port=pg_port,
        database=pg_database,
        password=pg_password
        )

        # Create a cursor object using the connection
        cursor = conn.cursor()
        data = [tuple(row) for row in df.values]

        # Define the insert query
        insert_query = f"INSERT INTO {table_name} VALUES %s"

        # Execute the insert query
        execute_values(cursor, insert_query, data)

        # Commit the transaction
        conn.commit()

        print("DataFrame stored in PostgreSQL database successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)