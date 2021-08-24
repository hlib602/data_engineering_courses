import logging
from settings import greenplum_conn

gp_url = f"jdbc:postgresql://{greenplum_conn.host}:{greenplum_conn.port}/{greenplum_conn.schema}"
gp_properties = {"user": greenplum_conn.login, "password": greenplum_conn.password}

def loadLocationAreasTableToGold(spark, date):
    location_areas_df = spark.read.parquet(f"/dshop_datalake/silver/location_areas/{date}")
    location_areas_df.write.jdbc(gp_url, table='location_areas', properties=gp_properties, mode='overwrite')
    logging.info("The table 'location areas' successfully loaded to warehouse")

def loadClientsTableToGold(spark, date):
    clients_df = spark.read.parquet(f"/dshop_datalake/silver/clients/{date}")
    clients_df.write.jdbc(gp_url, table='clients', properties=gp_properties, mode='overwrite')
    logging.info("The table 'clients' successfully loaded to warehouse")

def loadStoresTableToGold(spark, date):
    stores_df = spark.read.parquet(f"/dshop_datalake/silver/stores/{date}")
    stores_df.write.jdbc(gp_url, table='stores', properties=gp_properties, mode='overwrite')
    logging.info("The table 'stores' successfully loaded to warehouse")

def loadOrdersTableToGold(spark, date):
    orders_df = spark.read.parquet(f"/dshop_datalake/silver/orders/{date}")
    orders_df.write.jdbc(gp_url, table='orders', properties=gp_properties, mode='overwrite')
    logging.info("The table 'orders' successfully loaded to warehouse")

def loadDatesTableToGold(spark, date):
    dates_df = spark.read.parquet(f"/dshop_datalake/silver/dates/{date}")
    dates_df.write.jdbc(gp_url, table='dates', properties=gp_properties, mode='overwrite')
    logging.info("The table 'dates' successfully loaded to warehouse")

def loadProductsTableToGold(spark, date):
    products_df = spark.read.parquet(f"/dshop_datalake/silver/products/{date}")
    products_df.write.jdbc(gp_url, table='products', properties=gp_properties, mode='overwrite')
    logging.info("The table 'products' successfully loaded to warehouse")

def loadAPIDataToGold(spark, date):
    api_df = spark.read.parquet(f"/api_datalake/silver/{date}")
    api_df.write.jdbc(gp_url, table='out_of_stock', properties=gp_properties, mode='append')
    logging.info("The table 'out_of_stock' successfully loaded to warehouse")
