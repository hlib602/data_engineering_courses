from hdfs_module import loadAPIDataToBronze
from spark_module import loadAPIDataToSilver
from greenplum_module import loadAPIDataToGold
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from settings import spark

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag1 = DAG(
    'load_api_data_dag',
    description='The dag for loading data from API to HDFS',
    schedule_interval='@daily',
    start_date=datetime(2021,8,22,12,0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='load_api_data_to_bronze',
    dag=dag1,
    python_callable=loadAPIDataToBronze,
    op_kwargs= {'date' : datetime.now().strftime("%Y-%m-%d")}
)

t2 = PythonOperator(
    task_id='load_api_data_to_silver',
    dag=dag1,
    python_callable=loadAPIDataToSilver,
    op_kwargs= {'spark' : spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

t3 = PythonOperator(
    task_id='load_api_data_to_gold',
    dag=dag1,
    python_callable=loadAPIDataToGold,
    op_kwargs= {'spark' : spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

t1 >> t2 >> t3
