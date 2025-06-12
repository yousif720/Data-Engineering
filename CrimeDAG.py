from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import logging
import folium
from folium.plugins import HeatMap
import pandas as pd
import numpy as np
import os
import requests


CSV_DIR = '/home/yousif/datasets/'
CRIME_2010_URL = 'https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD'
ARREST_2010_URL = 'https://data.lacity.org/api/views/yru6-6re4/rows.csv?accessType=DOWNLOAD'
CRIME_2020_URL = 'https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD'
ARREST_2020_URL = 'https://data.lacity.org/api/views/amvf-fr72/rows.csv?accessType=DOWNLOAD'
DUCKDB_PATH = '/home/yousif/Database/lapd_new.db'

def download_file(url, destination):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(destination, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def check_and_download_files(**context):
    """Check and download required CSV files"""
    logger = context['ti'].log if 'ti' in context else logging.getLogger(__name__)
    
    try:
        os.makedirs(CSV_DIR, exist_ok=True)
        
        crime_2010_path = os.path.join(CSV_DIR, 'Crime2010.csv')
        if not os.path.exists(crime_2010_path):
            logger.info("Downloading Crime2010.csv")
            download_file(CRIME_2010_URL, crime_2010_path)
            logger.info(f"Successfully downloaded to {crime_2010_path}")
        else:
            logger.info("Crime2010.csv already exists - skipping download")
        
        arrest_2010_path = os.path.join(CSV_DIR, 'Arrest2010.csv')
        if not os.path.exists(arrest_2010_path):
            logger.info("Downloading Arrest2010.csv")
            download_file(ARREST_2010_URL, arrest_2010_path)
            logger.info(f"Successfully downloaded to {arrest_2010_path}")
        else:
            logger.info("Arrest2010.csv already exists - skipping download")
        
        crime_2020_path = os.path.join(CSV_DIR, 'Crime2020.csv')
        logger.info("Downloading Crime2020.csv")
        download_file(CRIME_2020_URL, crime_2020_path)
        logger.info(f"Successfully downloaded to {crime_2020_path}")
        
        arrest_2020_path = os.path.join(CSV_DIR, 'Arrest2020.csv')
        logger.info("Downloading Arrest2020.csv")
        download_file(ARREST_2020_URL, arrest_2020_path)
        logger.info(f"Successfully downloaded to {arrest_2020_path}")
        
        required_files = [crime_2010_path, arrest_2010_path, crime_2020_path, arrest_2020_path]
        for file_path in required_files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Required file {file_path} was not downloaded")
        
        logger.info("All required CSV files are ready")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download files: {str(e)}")
        raise


def rename_database_file():
    old_path = '/home/yousif/Database/lapd_new.db'
    new_path = '/home/yousif/Database/lapd.db'
    
    try:
        con = duckdb.connect(old_path)
        con.close()
    except:
        pass
    
    if os.path.exists(new_path):
        os.remove(new_path)
    
    os.rename(old_path, new_path)
    
    old_wal = old_path + '.wal'
    new_wal = new_path + '.wal'
    if os.path.exists(old_wal):
        os.rename(old_wal, new_wal)


def initial_load(csv_path, table_name, is_arrest_data=False):
    con = duckdb.connect(DUCKDB_PATH)
    if is_arrest_data:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT 
                * EXCLUDE ("Arrest Date", "Time", "Booking Date", "Booking Time"),
                
                -- Process Arrest DateTime
                CASE 
                    WHEN "Time" = '2400' THEN
                        strptime("Arrest Date"::VARCHAR, '%Y-%m-%d') + interval 1 day
                    ELSE
                        strptime("Arrest Date"::VARCHAR || ' ' || 
                                substr("Time"::VARCHAR, 1, 2) || ':' || substr("Time"::VARCHAR, 3, 2),
                                '%Y-%m-%d %H:%M')
                END AS "ArrestDateTime",
                
                -- Process Booking DateTime
                CASE 
                    WHEN "Booking Time" = '2400' THEN
                        strptime(substr("Booking Date"::VARCHAR, 1, 10), '%Y-%m-%d') + interval 1 day
                    ELSE
                        strptime(substr("Booking Date"::VARCHAR, 1, 10) || ' ' || 
                                substr("Booking Time"::VARCHAR, 1, 2) || ':' || substr("Booking Time"::VARCHAR, 3, 2),
                                '%Y-%m-%d %H:%M')
                END AS "BookingDateTime"
            FROM read_csv_auto('{csv_path}');
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_path}');
        """)
    con.close()


def initial_loadArrest2020(csv_path, table_name):
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT 
            * EXCLUDE ("Arrest Date", "Time", "Booking Date", "Booking Time"),
            
            -- Process Arrest DateTime
            CASE 
                WHEN "Time" = '2400' THEN
                    strptime(substr("Arrest Date"::VARCHAR, 1, 10), '%Y-%m-%d') + interval 1 day
                ELSE
                    strptime(substr("Arrest Date"::VARCHAR, 1, 10) || ' ' || 
                            substr("Time"::VARCHAR, 1, 2) || ':' || substr("Time"::VARCHAR, 3, 2),
                            '%Y-%m-%d %H:%M')
            END AS "ArrestDateTime",
            
            -- Process Booking DateTime
            CASE 
                WHEN "Booking Time" = '2400' THEN
                    strptime(substr("Booking Date"::VARCHAR, 1, 10), '%Y-%m-%d') + interval 1 day
                ELSE
                    strptime(substr("Booking Date"::VARCHAR, 1, 10) || ' ' || 
                            substr("Booking Time"::VARCHAR, 1, 2) || ':' || substr("Booking Time"::VARCHAR, 3, 2),
                            '%Y-%m-%d %H:%M')
            END AS "BookingDateTime"
        FROM read_csv_auto('{csv_path}');
    """)
    con.close()

def append_data(csv_path, table_name, is_arrest_data=False):
    con = duckdb.connect(DUCKDB_PATH)
    if is_arrest_data:
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT 
                * EXCLUDE ("Arrest Date", "Time", "Booking Date", "Booking Time"),
                
                -- Process Arrest DateTime
                CASE 
                    WHEN "Time" = '2400' THEN
                        strptime("Arrest Date"::VARCHAR, '%Y-%m-%d') + interval 1 day
                    ELSE
                        strptime("Arrest Date"::VARCHAR || ' ' || 
                                substr("Time"::VARCHAR, 1, 2) || ':' || substr("Time"::VARCHAR, 3, 2),
                                '%Y-%m-%d %H:%M')
                END AS "ArrestDateTime",
                
                -- Process Booking DateTime
                CASE 
                    WHEN "Booking Time" = '2400' THEN
                        strptime(substr("Booking Date"::VARCHAR, 1, 10), '%Y-%m-%d') + interval 1 day
                    ELSE
                        strptime(substr("Booking Date"::VARCHAR, 1, 10) || ' ' || 
                                substr("Booking Time"::VARCHAR, 1, 2) || ':' || substr("Booking Time"::VARCHAR, 3, 2),
                                '%Y-%m-%d %H:%M')
                END AS "BookingDateTime"
            FROM read_csv_auto('{csv_path}');
        """)
    else:
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM read_csv_auto('{csv_path}');
        """)
    con.close()

def combine_arrest_tables():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
            CREATE OR REPLACE TABLE arrest AS
            SELECT * FROM arrest2010
            UNION ALL
            SELECT * FROM arrest2020;
            
            DROP TABLE IF EXISTS arrest2010;
            DROP TABLE IF EXISTS arrest2020;
        """)
    con.close()


def rename_columns(table_name):
    con = duckdb.connect(DUCKDB_PATH)
    df_columns = con.execute(f"PRAGMA table_info('{table_name}')").df()
    for col in df_columns['name']:
        clean_col = col.replace(" ", "")
        if clean_col != col:
            con.execute(f'ALTER TABLE {table_name} RENAME COLUMN "{col}" TO "{clean_col}";')
    con.close()

def fix_crime_dates(table_name):
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(f"""
        ALTER TABLE {table_name} ADD COLUMN DATETIMEOCC TIMESTAMP;
        UPDATE {table_name} 
        SET DATETIMEOCC = 
            CASE 
                WHEN TIMEOCC = 2400 THEN
                    strptime(substr(DATEOCC::VARCHAR, 1, 10), '%Y-%m-%d') + interval 1 day
                ELSE
                    strptime(substr(DATEOCC::VARCHAR, 1, 10) || ' ' || 
                                substr(TIMEOCC::VARCHAR, 1, 2) || ':' || substr(TIMEOCC::VARCHAR, 3, 2),
                                '%Y-%m-%d %H:%M')
            END;

            ALTER TABLE {table_name} DROP COLUMN DATEOCC;
            ALTER TABLE {table_name} DROP COLUMN TIMEOCC;
            ALTER TABLE {table_name} ALTER COLUMN DateRptd TYPE DATE USING CAST(DateRptd AS DATE);
    """)
    con.close()


def filter_invalid_crime_entries(**context):
    con = duckdb.connect(DUCKDB_PATH)
    logger = context['ti'].log if 'ti' in context else logging.getLogger(__name__)

    con.execute("""
        CREATE OR REPLACE TABLE crime_filtered AS
        SELECT *
        FROM crime
        WHERE VictAge BETWEEN 0 AND 120
          AND VictSex IN ('M', 'F');
        
        -- Replace the original table with the filtered one
        DROP TABLE crime;
        ALTER TABLE crime_filtered RENAME TO crime;
    """)

    removed = con.execute("""
        SELECT COUNT(*) 
        FROM crime
        WHERE VictAge NOT BETWEEN 0 AND 120
           OR VictSex NOT IN ('M', 'F')
    """).fetchone()[0]
    
    if removed > 0:
        logger.info(f"Filtered out {removed} crime records with invalid age or sex values")
    
    con.close()

def filter_invalid_arrest_entries(**context):
    con = duckdb.connect(DUCKDB_PATH)
    logger = context['ti'].log if 'ti' in context else logging.getLogger(__name__)

    con.execute("""
        CREATE OR REPLACE TABLE arrest_filtered AS
        SELECT *
        FROM arrest
        WHERE Age BETWEEN 0 AND 120
          AND SexCode IN ('M', 'F');
        
        -- Replace the original table with the filtered one
        DROP TABLE arrest;
        ALTER TABLE arrest_filtered RENAME TO arrest;
    """)
    
    removed = con.execute("""
        SELECT COUNT(*) 
        FROM arrest
        WHERE Age NOT BETWEEN 0 AND 120
           OR SexCode NOT IN ('M', 'F')
    """).fetchone()[0]
    
    if removed > 0:
        logger.info(f"Filtered out {removed} arrest records with invalid age or sex values")
    
    con.close()

def create_constrained_tables():
    con = duckdb.connect(DUCKDB_PATH)
    
    con.execute("""
        CREATE OR REPLACE TABLE crime_with_pk (
            DR_NO INTEGER PRIMARY KEY,
            DateRptd DATE,
            AREA SMALLINT,
            AREANAME VARCHAR,
            RptDistNo SMALLINT,
            Part1_2 SMALLINT,
            CrmCd SMALLINT,
            CrmCdDesc VARCHAR,
            Mocodes VARCHAR,
            VictAge SMALLINT,
            VictSex CHAR(1),
            VictDescent CHAR(1),
            PremisCd SMALLINT,
            PremisDesc VARCHAR,
            WeaponUsedCd INTEGER,
            WeaponDesc VARCHAR,
            Status VARCHAR,
            StatusDesc VARCHAR,
            CrmCd1 SMALLINT,
            CrmCd2 SMALLINT,
            CrmCd3 SMALLINT,
            CrmCd4 SMALLINT,
            LOCATION VARCHAR,
            CrossStreet VARCHAR,
            LAT FLOAT,
            LON FLOAT,
            DATETIMEOCC TIMESTAMP
        );
        
        INSERT INTO crime_with_pk
        SELECT * FROM crime;
        
        DROP TABLE crime;
        ALTER TABLE crime_with_pk RENAME TO crime;
    """)
    
    con.execute("""
        CREATE OR REPLACE TABLE arrest_with_pk (
            ReportID INTEGER PRIMARY KEY,
            ReportType VARCHAR,
            AreaID SMALLINT,
            AreaName VARCHAR,
            ReportingDistrict SMALLINT,
            Age BIGINT,
            SexCode CHAR(1),
            DescentCode CHAR(1),
            ChargeGroupCode SMALLINT,
            ChargeGroupDescription VARCHAR,
            ArrestTypeCode VARCHAR,
            Charge VARCHAR,
            ChargeDescription VARCHAR,
            DispositionDescription VARCHAR,
            Address VARCHAR,
            CrossStreet VARCHAR,
            LAT DOUBLE,
            LON DOUBLE,
            Location VARCHAR,
            BookingLocation VARCHAR,
            BookingLocationCode BIGINT,
            ArrestDateTime TIMESTAMP,
            BookingDateTime TIMESTAMP
        );
        
        INSERT INTO arrest_with_pk
        SELECT * FROM arrest;
        
        DROP TABLE arrest;
        ALTER TABLE arrest_with_pk RENAME TO arrest;
    """)
    
    con.close()

add_constraints = PythonOperator(
    task_id='add_constraints',
    python_callable=create_constrained_tables
)

def add_was_arrested_column():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        -- Add the new column with default false value
        ALTER TABLE crime ADD COLUMN wasArrested BOOLEAN DEFAULT false;
        
        -- Update the column for crimes that have matching arrests
        UPDATE crime
        SET wasArrested = true
        WHERE DR_NO IN (SELECT ReportID FROM arrest);
    """)
    
    con.close()


def vacuum_database():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("VACUUM;")
    con.close()

def generate_neighborhood_metrics():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE OR REPLACE TABLE neighborhood_safety AS
        WITH crime_stats AS (
            SELECT 
                AREANAME AS neighborhood,
                COUNT(*) AS total_crimes,
                SUM(CASE WHEN wasArrested THEN 1 ELSE 0 END) AS cleared_crimes,
                COUNT(DISTINCT CrmCdDesc) AS unique_crime_types,
                AVG(VictAge) AS avg_victim_age
            FROM crime
            GROUP BY AREANAME
        ),
        arrest_stats AS (
            SELECT 
                AreaName AS neighborhood,
                COUNT(*) AS total_arrests,
                COUNT(DISTINCT ChargeGroupDescription) AS unique_charge_types,
                AVG(Age) AS avg_arrestee_age
            FROM arrest
            GROUP BY AreaName
        )
        SELECT
            COALESCE(c.neighborhood, a.neighborhood) AS neighborhood,
            c.total_crimes,
            a.total_arrests,
            ROUND(c.cleared_crimes * 100.0 / NULLIF(c.total_crimes, 0), 2) AS clearance_rate,
            -- Safety score formula (adjust weights as needed)
            ROUND(
                (COALESCE(a.total_arrests, 0) * 0.4) + 
                (COALESCE(c.cleared_crimes, 0) * 0.6) / 
                NULLIF(c.total_crimes, 1), 2
            ) / 1000 AS safety_score,
            c.unique_crime_types,
            a.unique_charge_types,
            c.avg_victim_age,
            a.avg_arrestee_age
        FROM crime_stats c
        FULL OUTER JOIN arrest_stats a ON c.neighborhood = a.neighborhood
        ORDER BY safety_score DESC;
    """)
    con.close()

def export_to_parquet():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(f"""
        COPY crime TO '/home/yousif/Database/crime.parquet' (FORMAT PARQUET);
    """)
    
    con.execute(f"""
        COPY arrest TO '/home/yousif/Database/arrest.parquet' (FORMAT PARQUET);
    """)

    con.execute(f"""
        COPY neighborhood_safety TO '/home/yousif/Database/neighborhood_safety.parquet' (FORMAT PARQUET);
    """)
    con.close()

def generate_crime_heatmap(**context):
    con = duckdb.connect(DUCKDB_PATH)
    
    crime_df = con.execute("""
        SELECT LAT, LON, CrmCdDesc, DATETIMEOCC
        FROM crime
        WHERE LAT IS NOT NULL AND LON IS NOT NULL
    """).df()
    
    la_map = folium.Map(location=[34.0522, -118.2437], zoom_start=11)
    
    heat_data = crime_df[['LAT', 'LON']].values.tolist()
    HeatMap(heat_data, radius=15).add_to(la_map)
    
    map_path = '/home/yousif/Database/crime_heatmap.html'
    la_map.save(map_path)
    
    context['ti'].log.info(f"Heatmap saved to {map_path}")
    con.close()

def generate_arrest_heatmap(**context):
    con = duckdb.connect(DUCKDB_PATH)
    
    arrest_df = con.execute("""
        SELECT LAT, LON, ChargeGroupDescription, ArrestDateTime
        FROM arrest
        WHERE LAT IS NOT NULL AND LON IS NOT NULL
    """).df()
    la_map = folium.Map(location=[34.0522, -118.2437], zoom_start=11)
    heat_data = arrest_df[['LAT', 'LON']].values.tolist()
    HeatMap(heat_data, radius=15, gradient={0.4: 'blue', 0.6: 'lime', 1: 'red'}).add_to(la_map)
    map_path = '/home/yousif/Database/arrest_heatmap.html'
    la_map.save(map_path)
    
    context['ti'].log.info(f"Arrest heatmap saved to {map_path}")
    con.close()


def generate_crime_arrest_ratio_heatmap(**context):

    con = duckdb.connect(DUCKDB_PATH)
    ratio_df = con.execute("""
        WITH crime_coords AS (
            SELECT 
                AREANAME,
                ROUND(LAT, 4) AS lat,
                ROUND(LON, 4) AS lon,
                COUNT(*) AS crimes
            FROM crime
            WHERE LAT IS NOT NULL AND LON IS NOT NULL
            GROUP BY AREANAME, ROUND(LAT, 4), ROUND(LON, 4)
        ),
        arrest_coords AS (
            SELECT 
                AreaName,
                ROUND(LAT, 4) AS lat,
                ROUND(LON, 4) AS lon,
                COUNT(*) AS arrests
            FROM arrest
            WHERE LAT IS NOT NULL AND LON IS NOT NULL
            GROUP BY AreaName, ROUND(LAT, 4), ROUND(LON, 4)
        )
        SELECT
            COALESCE(c.AREANAME, a.AreaName) AS area,
            COALESCE(c.lat, a.lat) AS lat,
            COALESCE(c.lon, a.lon) AS lon,
            c.crimes,
            a.arrests,
            -- Ratio calculation with smoothing to avoid division by zero
            ROUND(
                (COALESCE(a.arrests, 0) + 1) / 
                (COALESCE(c.crimes, 0) + 1), 
            3
            ) AS arrest_ratio
        FROM crime_coords c
        FULL OUTER JOIN arrest_coords a 
            ON c.AREANAME = a.AreaName 
            AND c.lat = a.lat 
            AND c.lon = a.lon
    """).df()
    
    ratio_df['normalized_ratio'] = np.log1p(ratio_df['arrest_ratio'])
    
    la_map = folium.Map(location=[34.0522, -118.2437], zoom_start=11)
    
    heat_data = ratio_df[['lat', 'lon', 'normalized_ratio']].values.tolist()
    HeatMap(
        heat_data,
        radius=15,
        gradient={0.1: 'green', 0.5: 'yellow', 0.9: 'red'},
        min_opacity=0.3,
        blur=15
    ).add_to(la_map)
    
    for _, row in ratio_df.nlargest(5, 'arrest_ratio').iterrows():
        folium.Marker(
            [row['lat'], row['lon']],
            popup=f"High Arrest Rate: {row['arrest_ratio']:.0%}",
            icon=folium.Icon(color='green')
        ).add_to(la_map)
    
    for _, row in ratio_df.nsmallest(5, 'arrest_ratio').iterrows():
        folium.Marker(
            [row['lat'], row['lon']],
            popup=f"Low Arrest Rate: {row['arrest_ratio']:.0%}",
            icon=folium.Icon(color='red')
        ).add_to(la_map)
    
    map_path = '/home/yousif/Database/crime_arrest_ratio_heatmap.html'
    la_map.save(map_path)
    
    context['ti'].log.info(f"Crime-arrest ratio heatmap saved to {map_path}")
    con.close()


with DAG(
    dag_id='LAPDFinal',
    start_date=datetime(2025, 6, 1), 
    schedule_interval='0 0 1 * *',
    catchup=False
) as dag:
    
    download_files = PythonOperator(
        task_id='download_files',
        python_callable=check_and_download_files,
        provide_context=True
    )

    load_crime = PythonOperator(
        task_id='load_crime',
        python_callable=initial_load,
        op_args=['/home/yousif/datasets/Crime2010.csv', 'crime', False]
    )

    append_crime = PythonOperator(
        task_id='append_crime',
        python_callable=append_data,
        op_args=['/home/yousif/datasets/Crime2020.csv', 'crime', False]
    )

    rename_crime = PythonOperator(
        task_id='rename_crime',
        python_callable=rename_columns,
        op_args=['crime']
    )

    add_datetimeocc = PythonOperator(
        task_id='add_datetimeocc',
        python_callable=fix_crime_dates,
        op_args=['crime']
    )

    filter_crime_data = PythonOperator(
        task_id='filter_crime_data',
        python_callable=filter_invalid_crime_entries
    )

   
    load_arrest = PythonOperator(
        task_id='load_arrest',
        python_callable=initial_load,
        op_args=['/home/yousif/datasets/Arrest2010.csv', 'arrest2010', True]
    )

    load_arrest2 = PythonOperator(
        task_id='2020_arrest',
        python_callable=initial_loadArrest2020,
        op_args=['/home/yousif/datasets/Arrest2020.csv', 'arrest2020']
    )

    combine_arrest = PythonOperator(
        task_id='combine_arrest',
        python_callable=combine_arrest_tables
    )

    rename_arrest = PythonOperator(
        task_id='rename_arrest',
        python_callable=rename_columns,
        op_args=['arrest']
    )

    filter_arrest_data = PythonOperator(
        task_id='filter_arrest_data',
        python_callable=filter_invalid_arrest_entries
    )

    add_arrested_flag = PythonOperator(
        task_id='add_arrested_flag',
        python_callable=add_was_arrested_column
    )
    
    vacuum_db = PythonOperator(
        task_id='vacuum_db',
        python_callable=vacuum_database
    )

    export_parquet = PythonOperator(
        task_id='export_parquet',
        python_callable=export_to_parquet
    )

    neighborhood_metrics = PythonOperator(
        task_id='generate_neighborhood_metrics',
        python_callable=generate_neighborhood_metrics,
        provide_context=True
    )

    crime_heatmap = PythonOperator(
        task_id='generate_crime_heatmap',
        python_callable=generate_crime_heatmap,
        provide_context=True
    )

    arrest_heatmap = PythonOperator(
        task_id='generate_arrest_heatmap',
        python_callable=generate_arrest_heatmap,
        provide_context=True
    )

    ratio_heatmap = PythonOperator(
        task_id='generate_crime_arrest_ratio_heatmap',
        python_callable=generate_crime_arrest_ratio_heatmap,
        provide_context=True
    )

    rename_db_file = PythonOperator(
    task_id='rename_database_file',
    python_callable=rename_database_file
)

# Dependencies
download_files >> [load_crime, load_arrest]
load_crime >> append_crime >> rename_crime >> filter_crime_data >> add_datetimeocc
load_arrest >> load_arrest2 >> combine_arrest >> rename_arrest >> filter_arrest_data
[add_datetimeocc, filter_arrest_data] >> add_constraints >> add_arrested_flag >> [crime_heatmap, arrest_heatmap, neighborhood_metrics] >> ratio_heatmap >> vacuum_db >> export_parquet >> rename_db_file
