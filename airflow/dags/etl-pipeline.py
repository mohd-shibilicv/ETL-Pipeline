from covid_19 import extract, load
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
}


def etl_process():
    OWNER = "CSSEGISandData"
    REPO = "COVID-19"
    PATH = "csse_covid_19_data/csse_covid_19_daily_reports"
    URL = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}"
    db_name = "example"
    download_urls = extract(URL)
    load(download_urls, db_name, debug=True)


with DAG('etl_pipeline', default_args=default_args, schedule_interval=timedelta(minutes=1)) as dag:
    etl_task = PythonOperator(
        task_id="etl_task",
        python_callable=etl_process
    )

etl_task
