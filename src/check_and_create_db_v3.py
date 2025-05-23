import os
import psycopg2
from psycopg2 import sql
import subprocess

host = 'localhost'
user = 'postgres'
password = 'postgres'
database_name = 'city_bike_db_v3'
dump_file_path = 'city_bike_db_v3.dump'

def create_database_if_not_exists():
    try:
        connection = psycopg2.connect(
            host=host,
            database='postgres',
            user=user,
            password=password
        )
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [database_name])
            if cursor.fetchone():
                print(f"Database '{database_name}' already exists.")
            else:
                print(f"Database '{database_name}' does not exist. Creating it...")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                print(f"Database '{database_name}' created.")

                if os.path.exists(dump_file_path):
                    print(f"Restoring database from dump file: {dump_file_path}")
                    subprocess.run(['psql', '-U', user, '-h', host, '-d', database_name, '-f', dump_file_path], check=True)
                    print(f"Database restored from dump file.")

        connection.close()

        connection = psycopg2.connect(
            host=host,
            database=database_name,
            user=user,
            password=password
        )
        with connection.cursor() as cursor:
            create_member_dimension_table(cursor)
            create_rideable_dimension_table(cursor)
            create_date_dimension_table(cursor)
            create_station_dimension_table(cursor)
            create_time_dimension_table(cursor)
            create_ride_fact_table(cursor)
            connection.commit()

    except Exception as error:
        print(f"Error while creating the database: {error}")
    finally:
        if connection:
            connection.close()
            print("Connection closed.")

def create_member_dimension_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS member_dimension (
        id UUID PRIMARY KEY,
        type VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)
    print("Table 'member_dimension' created (or already exists).")

def create_rideable_dimension_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rideable_dimension (
        id UUID PRIMARY KEY,
        type VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)
    print("Table 'rideable_dimension' created (or already exists).")

def create_ride_fact_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ride_fact (
        id UUID PRIMARY KEY,
        member_type_id UUID,
        rideable_type_id UUID,
        start_station_id UUID,
        end_station_id UUID,
        start_date_id UUID,
        end_date_id UUID,
        trip_duration INT,
        distance DOUBLE PRECISION,
        speed DOUBLE PRECISION,
        FOREIGN KEY (member_type_id) REFERENCES member_dimension(id),
        FOREIGN KEY (rideable_type_id) REFERENCES rideable_dimension(id),
        FOREIGN KEY (start_station_id) REFERENCES station_dimension(id),
        FOREIGN KEY (end_station_id) REFERENCES station_dimension(id),
        FOREIGN KEY (start_date_id) REFERENCES date_dimension(id),
        FOREIGN KEY (end_date_id) REFERENCES date_dimension(id)
    );
    """
    cursor.execute(create_table_query)
    print("Table 'ride_fact' created (or already exists).")
    
def create_date_dimension_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS date_dimension (
        id UUID PRIMARY KEY,
        year INT,
        quarter INT,
        month INT,
        week INT,
        date TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    print("Table 'date_dimension' created (or already exists).")

def create_time_dimension_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS time_dimension (
        id UUID PRIMARY KEY,
        second INT,
        minute INT,
        hour INT
    );
    """
    cursor.execute(create_table_query)
    print("Table 'time_dimension' created (or already exists).")

def create_station_dimension_table(cursor):         
    create_table_query = """
    CREATE TABLE IF NOT EXISTS station_dimension (
        id UUID PRIMARY KEY,
        name VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    """
    cursor.execute(create_table_query)
    print("Table 'station_dimension' created (or already exists).")



def query_data():
    try:
        connection = psycopg2.connect(
            host=host,
            database=database_name,
            user=user,
            password=password
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM ride_fact LIMIT 10;")
            rows = cursor.fetchall()

            for row in rows:
                print(row)

    except Exception as error:
        print(f"Error querying the database: {error}")

    finally:
        if connection:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    create_database_if_not_exists()
    query_data()