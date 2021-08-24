from pyspark.sql import SparkSession
from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

http_conn = BaseHook.get_connection("api_connection")
greenplum_conn = BaseHook.get_connection("dshop_warehouse_conn")
spark = SparkSession.builder.config('spark.driver.extraClassPath'
            , '/home/user/shared/postgresql-42.2.23.jar').master('local').appName('FinalProject').getOrCreate()
client = InsecureClient(f'http://127.0.0.1:50070', user='user')
