import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
from typing import List, Tuple

from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import json 
from pandas import json_normalize #package for flattening json in pandas df
import numpy as np

from sqlalchemy import create_engine
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, Date, Identity, insert
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()



load_dotenv()

SNOWFLAKE_USER = "dalvisayali97"
SNOWFLAKE_PASSWORD = "Qwerty12345"
SNOWFLAKE_DATABASE = "NYCPHIL_DB"
SNOWFLAKE_ACCOUNT = "affjtns-bv95929"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_SCHEMA = "PUBLIC"


def print_df_info(dfs, df_names):
    for df, name in zip(dfs, df_names):
        num_rows, num_cols = df.shape
        columns = df.columns.tolist()
        print(f"DataFrame: {name}")
        print(f"Number of Rows: {num_rows}")
        print(f"Number of Columns: {num_cols}")
        print("Column Names:")
        for col in columns:
            print(f" - {col}")
        print("\n" + "-"*50 + "\n")


def calculate_null_percentages(dfs, df_names):
    null_percentages = {}

    for df, name in zip(dfs, df_names):
        null_counts = df.isnull().sum()
        total_rows = len(df)
        null_percentage = (null_counts / total_rows) * 100
        null_percentages[name] = null_percentage

    return null_percentages

# drop the columns with null % > 80
def drop_high_null_columns_from_dfs(dfs, df_names, null_percentages, threshold=80.0):
    cleaned_dfs = {}

    for df, name in zip(dfs, df_names):
        cols_to_drop = null_percentages[name][null_percentages[name] > threshold].index
        cleaned_df = df.drop(columns=cols_to_drop)
        cleaned_dfs[name] = cleaned_df

    return cleaned_dfs

######### Data cleaning

def clean_nycphil(nycphil_df):
    nycphil_df['programID'] = nycphil_df['programID'].astype(int)
    nycphil_df['id'] = nycphil_df['id'].astype(str)
    
    return nycphil_df


def clean_concerts(concerts_df):
    concerts_df['concert_Date'] = pd.to_datetime(concerts_df['concert_Date'], errors='coerce')
    concerts_df['concert_Time'] = concerts_df['concert_Time'].astype(str)
    concerts_df['programID'] = concerts_df['programID'].astype(int)
    concerts_df['id'] = concerts_df['id'].astype(str)
    
    return concerts_df


def clean_works(works_df):
    works_df.fillna({
        'work_workTitle': 'Unknown',
        'work_conductorName': 'Unknown',
        'work_composerName': 'Unknown',
        'work_movement': 'Unknown',
    }, inplace=True)
    
    works_df['work_ID'] = works_df['work_ID'].astype(str)
    works_df['programID'] = works_df['programID'].astype(int)
    works_df['id'] = works_df['id'].astype(str)
    
    return works_df


def clean_soloists(soloists_df):
    soloists_df['programID'] = soloists_df['programID'].astype(int)
    soloists_df['id'] = soloists_df['id'].astype(str)
    
    return soloists_df

## Connecting to snowflake
def connectionToSnow(connection_test=False):
    print("Loaded from env", os.getenv("SNOWFLAKE_ACCOUNT"))
    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/'.format(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account_identifier=SNOWFLAKE_ACCOUNT,
        ), connect_args={'client_session_keep_alive': True}
    )
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
        if connection_test:
            connection.close()
        else:
            connection.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            connection.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            return connection
    finally:
        engine.dispose()


def create_tables(engine):
    # Metadata object to hold table definitions
    metadata = MetaData()    

    # Table definitions
    nycphil = Table('nycphil', metadata,
        Column('id', String, primary_key=True),
        Column('season', String),
        Column('orchestra', String),
        Column('program_id', Integer)
    )

    concerts = Table('concerts', metadata,
        Column('concert_id', Integer, Identity(start=1, increment=1), primary_key=True),
        Column('concert_date', String),
        Column('concert_event_type', String),
        Column('concert_venue', String),
        Column('concert_location', String),
        Column('concert_time', String),
        Column('nycphil_id', String, ForeignKey('nycphil.id'))
    )

    works = Table('works', metadata,
        Column('work_id', Integer, Identity(start=1, increment=1), primary_key=True),
        Column('work_work_title', String),
        Column('work_conductor_name', String),
        Column('work_composer_name', String),
        Column('work_movement', String),
        Column('nycphil_id', String, ForeignKey('nycphil.id'))
    )

    soloists = Table('soloists', metadata,
        Column('soloist_id', Integer, Identity(start=1, increment=1), primary_key=True),
        Column('soloist_name', String),
        Column('soloist_roles', String),
        Column('soloist_instrument', String),
        Column('nycphil_id', String, ForeignKey('nycphil.id'))
    )

    # Create tables in Snowflake
    metadata.create_all(engine)
    # Base.metadata.create_all(engine)
    return nycphil, concerts, works, soloists


def export_to_csv(dataframes, folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    for table_name, df in dataframes.items():
        file_path = os.path.join(folder_path, f"{table_name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Exported {table_name} to {file_path}")


def execute(connection,query):
    try:
        print(query)
        results = connection.execute(query)
    except Exception as e:
        print("error-->",e)
    finally:
        print("Done")

def preprocess_data():
    
    print("START!!!!!!!!!!")
    #load json object
    print("\n" + "="*50 + "\n")
    print("\nSTEP 1: LOADING JSON DATA")
    with open('output_data/raw_nyc_phil.json') as f:
        data = json.load(f)

    print("\n" + "="*50 + "\n")
    print("\nSTEP 2: NORMALIZING JSON DATA INTO DATAFRAMES")
    nycphil = json_normalize(data['programs'])
    # Normalize concerts, works, and soloists
    concerts_df = pd.json_normalize(data=data['programs'], record_path='concerts', meta=['season', 'orchestra', 'programID', 'id'], record_prefix='concert_')
    works_df = pd.json_normalize(data=data['programs'], record_path='works', meta=['season', 'orchestra', 'programID', 'id'], record_prefix='work_')
    soloists_df = pd.json_normalize(data['programs'], record_path=['works', 'soloists'], meta=['season', 'orchestra', 'programID', 'id'], record_prefix='soloist_')

    print_df_info([nycphil, concerts_df, works_df, soloists_df], ['nycphil', 'concerts_df', 'works_df', 'soloists_df'])

    print("\n" + "="*50 + "\n")
    print("\nSTEP 3: DATA EXPLORATION")
    print("Checking the null percentage of each column in all dfs...")
    null_percentages = calculate_null_percentages([nycphil, concerts_df, works_df, soloists_df], 
                                              ['nycphil', 'concerts_df', 'works_df', 'soloists_df'])
    print(null_percentages)
    
    print("Dropping columns with null % > 80...")
    cleaned_dfs = drop_high_null_columns_from_dfs([nycphil, concerts_df, works_df, soloists_df], 
                                ['nycphil', 'concerts_df', 'works_df', 'soloists_df'],
                               null_percentages)

    cleaned_nycphil = cleaned_dfs['nycphil']
    cleaned_concerts_df = cleaned_dfs['concerts_df']
    cleaned_works_df = cleaned_dfs['works_df']
    cleaned_soloists_df = cleaned_dfs['soloists_df']

    print("\n" + "="*50 + "\n")
    print("\nSTEP 4: DATA CLEANING")
    print("Replacing nulls with 'Unknown' value and converting into correct data types...")
    cleaned_nycphil = clean_nycphil(cleaned_nycphil)
    cleaned_concerts_df = clean_concerts(cleaned_concerts_df)
    cleaned_works_df = clean_works(cleaned_works_df)        
    cleaned_soloists_df = clean_soloists(cleaned_soloists_df)

    print_df_info([cleaned_nycphil, cleaned_concerts_df, cleaned_works_df, cleaned_soloists_df], ['nycphil', 'concerts_df', 'works_df', 'soloists_df'])

    df_nycphil = pd.DataFrame({
        'id': cleaned_nycphil['id'],
        'season': cleaned_nycphil['season'],
        'orchestra': cleaned_nycphil['orchestra'],
        'program_id': cleaned_nycphil['programID']
    })

    df_concerts = pd.DataFrame({
        'concert_date': cleaned_concerts_df['concert_Date'],
        'concert_event_type': cleaned_concerts_df['concert_eventType'],
        'concert_venue': cleaned_concerts_df['concert_Venue'],
        'concert_location': cleaned_concerts_df['concert_Location'],
        'concert_time': cleaned_concerts_df['concert_Time'],
        'nycphil_id': cleaned_concerts_df['id']
    })

    df_works = pd.DataFrame({
        'work_work_title': cleaned_works_df['work_workTitle'],
        'work_conductor_name': cleaned_works_df['work_conductorName'],
        'work_composer_name': cleaned_works_df['work_composerName'],
        'work_movement': cleaned_works_df['work_movement'],
        'nycphil_id': cleaned_concerts_df['id']
    })

    df_soloists = pd.DataFrame({
        'soloist_name': cleaned_soloists_df['soloist_soloistName'],
        'soloist_roles': cleaned_soloists_df['soloist_soloistRoles'],
        'soloist_instrument': cleaned_soloists_df['soloist_soloistInstrument'],
        'nycphil_id': cleaned_concerts_df['id']
    })

    # Dictionary of DataFrames
    dataframes = {
        "nycphil": df_nycphil,
        "concerts": df_concerts,
        "works": df_works,
        "soloists": df_soloists
    }

    # Export DataFrames to CSV
    print("\n" + "="*50 + "\n")
    print("\nSTEP 5: EXPORTING CLEANED DATA AS CSV")
    local_csv_folder = "cleaned_data/"
    export_to_csv(dataframes, local_csv_folder)

    print("\n" + "="*50 + "\n")
    print("\nSTEP 6: CREATING SNOWFLAKE TABLES")
    ## Uploading csv to Snowflake
    connection = connectionToSnow()
    nycphil_t, concerts_t, works_t, soloists_t = create_tables(connection)


    # Define the columns for each table (excluding the auto-incrementing ID column)
    table_columns = {
        "nycphil": ["id", "season", "orchestra", "program_id"],
        "concerts": ["concert_date", "concert_event_type", "concert_venue", "concert_location", "concert_time", "nycphil_id"],
        "works": ["work_work_title", "work_conductor_name", "work_composer_name", "work_movement", "nycphil_id"],
        "soloists": ["soloist_name", "soloist_roles", "soloist_instrument"]
    }

    print("\n" + "="*50 + "\n")
    print("\nSTEP 7: UPLOADING TO SNOWFLAKE")
    for file_name in os.listdir(local_csv_folder):
        if file_name.endswith(".csv"):
            file_path = os.path.join(local_csv_folder, file_name)
            table_name = file_name.replace(".csv", "")
            print(f"Creating stage {table_name}...")

            create_stage = """CREATE OR REPLACE STAGE {} DIRECTORY = ( ENABLE = true ) FILE_FORMAT = (TYPE = 'CSV' field_optionally_enclosed_by='"' SKIP_HEADER = 1);""".format(table_name)
            execute(connection, create_stage)
            
            print(f"Uploading {file_path} to stage {table_name}...")
            upload_to_stage  = """PUT file://{} @{}.public.{};""".format(file_path,SNOWFLAKE_DATABASE, table_name)
            execute(connection, upload_to_stage)
            
            print(f"Loading data from {file_name} to table {table_name}...")
            columns_str = ", ".join(table_columns[table_name])

            copy_stage_to_table = """COPY INTO {} ({})
            FROM @{}.public.{}
            FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' SKIP_HEADER = 1)
            """.format(table_name, columns_str,  SNOWFLAKE_DATABASE,table_name)
            execute(connection, copy_stage_to_table)

    print("DONE!!!!!!!!!!")


dag = DAG(
    dag_id="sandbox",
    schedule="0 0 * * *",   # https://crontab.guru/
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["Nomura", "nycphil"],
)
    

with dag:
    start = BashOperator(
        task_id = "start",
        bash_command = 'echo "Triggering airflow pipeline!!"'
    )

    preprocess_main = PythonOperator(
        task_id = 'Preprocess_data',
        python_callable = preprocess_data,
        provide_context = True,
        dag = dag,
    )
    
    # upload_gcp2snowflake_main = PythonOperator(
    #     task_id = 'Upload_from_GCP_to_Snowflake',
    #     python_callable = upload_gcp2snowflake_main,
    #     provide_context = True,
    #     dag = dag,
    # )
    
    # upload_recipeName2pinecone = PythonOperator(
    #     task_id='Upload_to_Pinecone_Recipe_Name_namespace',
    #     python_callable=upload_embeddings2pinecone_test,
    #     provide_context=True,
    #     op_args = [os.getenv('PINECONE_NAMESPACE_1') ,'name', 20],
    #     dag=dag,
    # )

    # upload_ingredients2pinecone = PythonOperator(
    #     task_id='Upload_to_Pinecone_Ingredient_namespace',
    #     python_callable=upload_embeddings2pinecone_test,
    #     provide_context=True,   
    #     op_args = [os.getenv('PINECONE_NAMESPACE_2'),'recipeingredientparts', 20],
    #     dag=dag,
    # )

    start >> preprocess_main
    # >> upload_csv2gcp_main >> upload_gcp2snowflake_main >> upload_recipeName2pinecone >> upload_ingredients2pinecone

