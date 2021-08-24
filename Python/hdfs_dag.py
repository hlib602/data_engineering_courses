from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from hdfs_module import loadTablesToBronze
from spark_module import loadClientsTableToSilver,loadLocationAreasTableToSilver\
    ,loadOrdersAndDatesTablesToSilver,loadProductsTableToSilver,loadStoresTableToSilver
from greenplum_module import loadClientsTableToGold,loadDatesTableToGold,loadLocationAreasTableToGold\
    ,loadOrdersTableToGold,loadProductsTableToGold,loadStoresTableToGold
from settings import spark

from datetime import datetime

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'load_dshop_database_tables_dag',
    description = 'The dag for loading Postgres data to HDFS',
    schedule_interval = '@daily',
    start_date = datetime(2021,8,22,12,0),
    default_args = default_args
)

pg_hook = PostgresHook(postgres_conn_id='dshop_bu_postgres')
connection = pg_hook.get_conn()
cursor = connection.cursor()
sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
cursor.execute(sql)
result = cursor.fetchall()

dummy1 = DummyOperator(task_id="dummy1",dag=dag)

tables = []
for i in range(len(result)):
    tables.append(result[i][0])

loadToBronze_tasks = []
for table in tables:
    loadToBronze_tasks.append(PythonOperator(
            task_id = 'loadToBronze_' + table + '_task',
            python_callable = loadTablesToBronze,
            dag = dag,
            op_kwargs= {'pg_hook': pg_hook, 'table' : table, 'date' : datetime.now().strftime("%Y-%m-%d")}
        )
    )

dummy2 = DummyOperator(task_id="dummy2",dag=dag)

loadLocationAreasToSilver = PythonOperator(
    task_id = 'loadToSilver_LocationAreas_task',
    python_callable = loadLocationAreasTableToSilver,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadClientsToSilver = PythonOperator(
    task_id = 'loadToSilver_Clients_task',
    python_callable = loadClientsTableToSilver,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadStoresToSilver = PythonOperator(
    task_id = 'loadToSilver_Stores_task',
    python_callable = loadStoresTableToSilver,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadOrdersAndDatesToSilver = PythonOperator(
    task_id = 'loadToSilver_OrdersAndDates_task',
    python_callable = loadOrdersAndDatesTablesToSilver,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadProductsToSilver = PythonOperator(
    task_id = 'loadToSilver_Products_task',
    python_callable = loadProductsTableToSilver,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

dummy3 = DummyOperator(task_id="dummy3",dag=dag)

loadLocationAreasToGold = PythonOperator(
    task_id = 'loadToGold_LocationAreas_task',
    python_callable = loadLocationAreasTableToGold,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadClientsToGold = PythonOperator(
    task_id = 'loadToGold_Clients_task',
    python_callable = loadClientsTableToGold,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadStoresToGold = PythonOperator(
    task_id = 'loadToGold_Stores_task',
    python_callable = loadStoresTableToGold,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadOrdersToGold = PythonOperator(
    task_id = 'loadToGold_Orders_task',
    python_callable = loadOrdersTableToGold,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadDatesToGold = PythonOperator(
    task_id = 'loadToGold_Dates_task',
    python_callable = loadDatesTableToGold,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

loadProductsToGold = PythonOperator(
    task_id = 'loadToGold_Products_task',
    python_callable = loadProductsTableToGold,
    dag = dag,
    op_kwargs = {'spark': spark, 'date' : datetime.now().strftime("%Y-%m-%d")}
)

dummy4 = DummyOperator(task_id="dummy4",dag=dag)

dummy1 >> loadToBronze_tasks >> dummy2 >>\
            [loadLocationAreasToSilver, loadClientsToSilver, loadStoresToSilver\
            , loadOrdersAndDatesToSilver, loadProductsToSilver]\
            >> dummy3 >> [loadLocationAreasToGold, loadClientsToGold, loadStoresToGold\
                          , loadOrdersToGold, loadDatesToGold, loadProductsToGold] >> dummy4


