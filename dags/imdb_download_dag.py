from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def download_imdb_data():
    url = "https://datasets.imdbws.com/title.basics.tsv.gz"
    local_file = "/opt/airflow/dags/files/title.basics.tsv.gz"
    response = requests.get(url)
    with open(local_file, "wb") as f:
        f.write(response.content)
    print(f"Downloaded {local_file}")

with DAG(
    dag_id="imdb_download_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    download_task = PythonOperator(
        task_id="download_imdb_data",
        python_callable=download_imdb_data
    )
