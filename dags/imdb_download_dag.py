from airflow import DAG
from airflow.operators.python import PythonOperator
import gzip
import psycopg2
from datetime import datetime
import requests

def download_imdb_data():
    url = "https://datasets.imdbws.com/title.basics.tsv.gz"
    local_file = "/opt/airflow/dags/files/title.basics.tsv.gz"
    response = requests.get(url)
    with open(local_file, "wb") as f:
        f.write(response.content)
    print(f"Downloaded {local_file}")

def load_data_into_postgres():
    # Path to downloaded files
    gz_file_path = "/opt/airflow/dags/files/title.basics.tsv.gz"

    # Decompress the file
    with gzip.open(gz_file_path, 'rt', encoding='utf-8') as f:
        lines = f.readlines()

    # Parse header and rows
    header = lines[0].strip().split('\t')
    rows = [line.strip().split('\t') for line in lines[1:1001]] # First 1000 for test

    # Connect to Postgres
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS imdb_title_basics (
            tconst TEXT,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear TEXT,
            endYear TEXT,
            runtimeMinutes TEXT,
            genres TEXT
        );
    """)
    conn.commit()

with DAG(
    dag_id="imdb_download_and_load_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    download_task = PythonOperator(
        task_id="download_imdb_data",
        python_callable=download_imdb_data
    )

    load_task = PythonOperator(
        task_id="load_into_into_postgres",
        python_callable=load_data_into_postgres
    )

download_task>>load_task