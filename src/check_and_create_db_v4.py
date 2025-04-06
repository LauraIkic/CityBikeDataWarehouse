import os
import uuid
import numpy as np

import pandas as pd
import psycopg2
from psycopg2 import sql
import subprocess

from sqlalchemy import create_engine, TIMESTAMP, String, Integer

host = 'localhost'
user = 'postgres'
password = 'postgres'
database_name = 'city_bike_db'
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
        member_type_id UUID,
        rideable_type_id UUID,
        start_station_id UUID,
        end_station_id UUID,
        start_date_id UUID,
        end_date_id UUID,
        trip_duration INT,
        distance DOUBLE PRECISION,
        speed DOUBLE PRECISION,
        PRIMARY KEY (member_type_id, rideable_type_id, start_station_id, end_station_id, start_date_id, end_date_id),
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
        day INT, 
        hour INT,
        minute INT,
        second INT,
        date TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    print("Table 'date_dimension' created (or already exists).")

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

def write_csv_to_database():
    file_path = 'city_bike_db.csv'
    df = pd.read_csv(file_path, delimiter=';')

    db_url = f"postgresql+psycopg2://{user}:{password}@localhost:5432/{database_name}"
    engine = create_engine(db_url)

    ### 1. Member Dimension ###
    member_df = df[['member_casual']].drop_duplicates().reset_index(drop=True)
    member_df['id'] = [str(uuid.uuid4()) for _ in range(len(member_df))]
    member_df.columns = ['type', 'id']
    member_df = member_df[['id', 'type']]
    member_df.to_sql('member_dimension', con=engine, if_exists='append', index=False)
    print("data inserted successfully into member_dimension table")

    ### 2. Rideable Dimension ###
    rideable_df = df[['rideable_type']].drop_duplicates().reset_index(drop=True)
    rideable_df['id'] = [str(uuid.uuid4()) for _ in range(len(rideable_df))]
    rideable_df.columns = ['type', 'id']
    rideable_df = rideable_df[['id', 'type']]
    rideable_df.to_sql('rideable_dimension', con=engine, if_exists='append', index=False)
    print("data inserted successfully into rideable_dimension table")

    ### 3. Station Dimension ###
    # Start Stations
    start_station_df = df[['start_station_name', 'start_lat', 'start_lng']].drop_duplicates()
    start_station_df['id'] = [str(uuid.uuid4()) for _ in range(len(start_station_df))]
    start_station_df.columns = ['name', 'latitude', 'longitude', 'id']
    start_station_df = start_station_df[['id', 'longitude', 'latitude', 'name']]

    # End Stations
    end_station_df = df[['end_station_name', 'end_lat', 'end_lng']].drop_duplicates()
    end_station_df['id'] = [str(uuid.uuid4()) for _ in range(len(end_station_df))]
    end_station_df.columns = ['name', 'latitude', 'longitude', 'id']
    end_station_df = end_station_df[['id', 'longitude', 'latitude', 'name']]

    # Combine both
    station_df = pd.concat([start_station_df, end_station_df]).drop_duplicates().reset_index(drop=True)
    station_df['id'] = [str(uuid.uuid4()) for _ in range(len(station_df))]
    station_df = station_df[['id', 'name', 'latitude', 'longitude']]

    station_df.to_sql('station_dimension', con=engine, if_exists='append', index=False)
    print("data inserted successfully into station_dimension table")

    # Convert to datetime
    df['started_at'] = pd.to_datetime(df['started_at'], format='%Y-%m-%d %H:%M:%S.%f')
    df['ended_at'] = pd.to_datetime(df['ended_at'], format='%Y-%m-%d %H:%M:%S.%f')

    # Start Date
    start_date_df = df[['started_at']].copy()
    start_date_df['id'] = [str(uuid.uuid4()) for _ in range(len(start_date_df))]
    start_date_df.columns = ['date', 'id']

    # End Date
    end_date_df = df[['ended_at']].copy()
    end_date_df['id'] = [str(uuid.uuid4()) for _ in range(len(end_date_df))]
    end_date_df.columns = ['date', 'id']

    # Combine both
    date_df = pd.concat([start_date_df, end_date_df]).reset_index(drop=True)

    # Extract date components
    date_df['year'] = date_df['date'].dt.year
    date_df['month'] = date_df['date'].dt.month
    date_df['week'] = date_df['date'].dt.isocalendar().week
    date_df['quarter'] = date_df['date'].dt.quarter
    date_df['day'] = date_df['date'].dt.day
    date_df['hour'] = date_df['date'].dt.hour
    date_df['minute'] = date_df['date'].dt.minute
    date_df['second'] = date_df['date'].dt.second
    
    # Optional: remove duplicates if you want to avoid repeated dates
    date_df = date_df.drop_duplicates(subset=['date'])

    # Final column order (you can modify)
    date_df = date_df[['id', 'date', 'year', 'month', 'quarter', 'week', 'day', 'hour', 'minute', 'second']]

    # Insert into Postgres with explicit types
    date_df.to_sql(
        'date_dimension',
        con=engine,
        if_exists='append',
        index=False,
        dtype={
            'id': String,
            'date': TIMESTAMP,
            'year': Integer,
            'month': Integer,
            'quarter': Integer,
            'week': Integer,
            'day': Integer,
            'hour': Integer,
            'minute': Integer,
            'second': Integer,
        }
    )

    print("data inserted into date_dimension table with year, month, quarter, week columns")

    ### 4. Map IDs back to original DataFrame ###
    # Member
    df = df.merge(member_df, how='left', left_on='member_casual', right_on='type')
    df.rename(columns={'id': 'member_type_id'}, inplace=True)
    df.drop(columns=['type'], inplace=True)

    # Rideable
    df = df.merge(rideable_df, how='left', left_on='rideable_type', right_on='type', suffixes=('', '_rideable'))
    df.rename(columns={'id': 'rideable_type_id'}, inplace=True)
    df.drop(columns=['type'], inplace=True)

    # Start Station
    df = df.merge(station_df, how='left', left_on=['start_station_name', 'start_lat', 'start_lng'],
                  right_on=['name', 'latitude', 'longitude'])
    df.rename(columns={'id': 'start_s_id'}, inplace=True)
    df.drop(columns=['name', 'latitude', 'longitude'], inplace=True)

    # End Station
    df = df.merge(station_df, how='left', left_on=['end_station_name', 'end_lat', 'end_lng'],
                  right_on=['name', 'latitude', 'longitude'], suffixes=('', '_end'))
    df.rename(columns={'id': 'end_s_id'}, inplace=True)
    df.drop(columns=['name', 'latitude', 'longitude'], inplace=True)

    # ======================
    # Start Date
    df = df.merge(date_df, how='left', left_on='started_at', right_on='date')
    df.rename(columns={'id': 'start_date_id'}, inplace=True)
    df.drop(columns=['date', 'year', 'month', 'quarter', 'week'], inplace=True)

    # End Date
    df = df.merge(date_df, how='left', left_on='ended_at', right_on='date', suffixes=('', '_end_date'))
    df.rename(columns={'id': 'end_date_id'}, inplace=True)
    df.drop(columns=['date', 'year', 'month', 'quarter', 'week'], inplace=True)

    # ======================

    df = df.loc[:, ~df.columns.duplicated()]

    trip_duration = (df['ended_at'] - df['started_at']).dt.total_seconds()

    distance = haversine(df['start_lat'], df['start_lng'], df['end_lat'], df['end_lng'])
    avg_speed_kmh = distance / ((trip_duration/60)/ 60)  # km/h
    speed = avg_speed_kmh.fillna(0).replace([np.inf, -np.inf], 0)

    ### 6. Create ride_fact ###
    ride_fact_df = pd.DataFrame({
        'member_type_id': df['member_type_id'],
        'rideable_type_id': df['rideable_type_id'],
        'start_station_id': df['start_s_id'],
        'end_station_id': df['end_s_id'],
        'start_date_id': df['start_date_id'],
        'end_date_id': df['end_date_id'],
        'trip_duration': trip_duration,
        'speed': speed,
        'distance': distance,
    }).drop_duplicates()
                                                                                    
    ride_fact_df.to_sql('ride_fact', con=engine, if_exists='replace', index=False)


    print("✅ Data inserted successfully into ride_fact table.")

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c

if __name__ == "__main__":
    create_database_if_not_exists()
    write_csv_to_database()
