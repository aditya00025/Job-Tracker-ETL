from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

def job_etl():
    url = "https://remoteok.com/api"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    data = response.json()[1:]

    jobs = []
    for job in data:
        jobs.append({
            "job_id": str(job.get("id")),
            "date_posted": job.get("date"),
            "company": job.get("company"),
            "position": job.get("position"),
            "location": job.get("location", "").strip() or "not mentioned",
            "tags": ", ".join(job.get("tags", [])),
            "salary": job.get("salary", "").strip() or "Not Disclosed",
            "url": job.get("url")
        })

    df = pd.DataFrame(jobs)
    df["date_posted"] = pd.to_datetime(df["date_posted"]).dt.date
    df["tags"] = df["tags"].str.replace(",", "#")

    conn = psycopg2.connect(
        dbname="job_tracker",
        user="postgres",
        password="password",
        host="host.docker.internal",  # use this to access host PostgreSQL from Docker
        port="5432"
    )

    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO jobs (job_id, date_posted, company, position, location, tags, salary, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO NOTHING;
        """, (row['job_id'], row['date_posted'], row['company'], row['position'],
              row['location'], row['tags'], row['salary'], row['url']))
    conn.commit()
    cursor.close()
    conn.close()

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='job_tracker_etl_dag',
    default_args=default_args,
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',  # change to None for manual runs
    catchup=False
) as dag:
    run_etl = PythonOperator(
        task_id='run_job_etl',
        python_callable=job_etl
    )
    run_etl
