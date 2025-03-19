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

    # Path to downloaded file
    gz_file_path = "/opt/airflow/dags/files/title.basics.tsv.gz"

    # Connect to Postgres
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    # Create table if not exists
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

    # Decompress and read file
    with gzip.open(gz_file_path, 'rt', encoding='utf-8') as f:
        header = next(f)  # Skip header
        row_count = 0

        for line in f:
            if row_count >= 1000:  # Limit to first 1000 rows for test
                break

            values = line.strip().split('\t')
            if len(values) == 9:  # Ensure row has all columns
                cur.execute("""
                    INSERT INTO imdb_title_basics (
                        tconst, titleType, primaryTitle, originalTitle,
                        isAdult, startYear, endYear, runtimeMinutes, genres
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, values)

                row_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Total rows processed and inserted: {row_count}")


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