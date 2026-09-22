from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingest_igdb_bulk import fetch_all_igdb_games
from ingest_rawg_bulk import fetch_all_rawg_games

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def wrap_bulk_ingest_igdb(**kwargs):
    fetch_all_igdb_games()

def wrap_bulk_ingest_rawg(**kwargs):
    fetch_all_rawg_games()

with DAG(
    'bulk_pipeline',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    task_bulk_ingest_igdb = PythonOperator(
        task_id='bulk_ingest_igdb',
        python_callable=wrap_bulk_ingest_igdb,
        provide_context=True
    )

    task_bulk_ingest_rawg = PythonOperator(
        task_id='bulk_ingest_rawg',
        python_callable=wrap_bulk_ingest_rawg,
        provide_context=True
    )

    task_bulk_format_igdb = BashOperator(
        task_id='format_igdb_bulk',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/format_igdb_bulk.py {{ ds_nodash }}
        '''
    )

    task_bulk_format_rawg = BashOperator(
        task_id='format_rawg_bulk',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/format_rawg_bulk.py {{ ds_nodash }}
        '''
    )

    task_bulk_ingest_igdb >> task_bulk_format_igdb
    task_bulk_ingest_rawg >> task_bulk_format_rawg