from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingest_igdb_daily import ingest_igdb_daily
from ingest_rawg_daily import ingest_rawg_daily

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

def wrap_ingest_rawg(**kwargs):
    logical_date = kwargs['ds_nodash'] 
    ingest_rawg_daily(execution_date=logical_date)

def wrap_ingest_igdb(**kwargs):
    logical_date = kwargs['ds_nodash']
    ingest_igdb_daily(execution_date=logical_date)

with DAG(
    'daily_pipeline',
    default_args=default_args,
    schedule='@daily', 
    catchup=False, 
    max_active_runs=1
) as dag:

    task_ingest_rawg = PythonOperator(
        task_id='ingest_rawg',
        python_callable=wrap_ingest_rawg,
        provide_context=True
    )

    task_ingest_igdb = PythonOperator(
        task_id='ingest_igdb',
        python_callable=wrap_ingest_igdb,
        provide_context=True
    )
    task_format_igdb = BashOperator(
        task_id='format_igdb_daily',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 6g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/format_igdb_daily.py {{ ds_nodash }}
        '''
    )
    task_format_rawg = BashOperator(
        task_id='format_rawg_daily',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 6g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/format_rawg_daily.py {{ ds_nodash }}
        '''
    )
    task_format_steam_library = BashOperator(
        task_id='format_steam_library',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 12g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/format_steam_library.py {{ ds_nodash }}
        '''
    )
    
    task_combine = BashOperator(
        task_id='combine_datas',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 10g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --conf "spark.sql.shuffle.partitions=800" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0,io.delta:delta-spark_2.12:3.1.0 \
            /opt/spark_jobs/combine_igdb_rawg_steam.py {{ ds_nodash }}
        '''
    )

    task_train_ml = BashOperator(
        task_id='train_ml',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 14g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --conf "spark.sql.shuffle.partitions=800" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/train_game_recommender.py {{ ds_nodash }}
        '''
    )

    task_index_games_es = BashOperator(
        task_id='index_games_to_e',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 10g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0,io.delta:delta-spark_2.12:3.1.0 \
            /opt/spark_jobs/index_master_games_to_es.py
        '''
    )

    task_index_reco_es = BashOperator(
        task_id='index_reco_to_es',
        bash_command='''
        docker exec spark /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 10g \
            --conf "spark.jars.ivy=/tmp/.ivy" \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
            /opt/spark_jobs/index_recommendations_to_es.py
        '''
    )

    task_ingest_igdb >> task_format_igdb
    task_ingest_rawg >> task_format_rawg
    [task_format_rawg, task_format_igdb, task_format_steam_library] >> task_combine
    task_combine >> task_index_games_es 
    task_combine >> task_train_ml >> task_index_reco_es
