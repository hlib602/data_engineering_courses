import logging
import pyspark.sql.functions as F

def loadLocationAreasTableToSilver(spark, date):
    location_areas_df = spark.read.load(f"/dshop_datalake/bronze/location_areas/location_areas_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    location_areas_df = location_areas_df.where(F.col("area_id").isNotNull())\
        .where(F.col("area").isNotNull())
    location_areas_df = location_areas_df.dropDuplicates()
    location_areas_df.write.parquet(f"/dshop_datalake/silver/location_areas/{date}", mode='overwrite')
    logging.info(f"The table 'location_areas' for the date {date} loaded to silver successfully")

def loadClientsTableToSilver(spark, date):
    clients_df = spark.read.load(f"/dshop_datalake/bronze/clients/clients_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    clients_df = clients_df.where(F.col("id").isNotNull())\
        .where(F.col("fullname").isNotNull()).where(F.col("location_area_id").isNotNull())
    clients_df = clients_df.dropDuplicates()
    clients_df.write.partitionBy('location_area_id').parquet(f"/dshop_datalake/silver/clients/{date}", mode='overwrite')
    logging.info(f"The table 'clients' for the date {date} loaded to silver successfully")

def loadStoresTableToSilver(spark, date):
    stores_df = spark.read.load(f"/dshop_datalake/bronze/stores/stores_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    store_types_df = spark.read.load(f"/dshop_datalake/bronze/store_types/store_types_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    stores_df = stores_df.join(store_types_df, ['store_type_id']).drop('store_type_id')
    stores_df = stores_df.where(F.col("store_id").isNotNull())\
        .where(F.col("location_area_id").isNotNull()).where(F.col("type").isNotNull())
    stores_df=stores_df.dropDuplicates()
    stores_df.write.partitionBy('type').parquet(f"/dshop_datalake/silver/stores/{date}", mode='overwrite')
    logging.info(f"The table 'stores' for the date {date} loaded to silver successfully")

def loadOrdersAndDatesTablesToSilver(spark, date):
    orders_df = spark.read.load(f"/dshop_datalake/bronze/orders/orders_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    orders_df = orders_df.withColumn('order_date',orders_df.order_date.cast('date'))
    orders_df = orders_df.where(F.col("order_id").isNotNull())\
        .where(F.col("product_id").isNotNull()).where(F.col("client_id").isNotNull())\
        .where(F.col("quantity").isNotNull()).where(F.col("order_date").isNotNull())\
        .where(F.col("store_id").isNotNull())
    orders_df = orders_df.dropDuplicates()
    orders_df.write.partitionBy('store_id').parquet(f"/dshop_datalake/silver/orders/{date}", mode='overwrite')
    logging.info(f"The table 'orders' for the date {date} loaded to silver successfully")

    dates_df=orders_df.select('order_date').distinct()
    dates_df=dates_df.select(F.col("order_date")\
                             ,F.dayofmonth(F.col("order_date")).alias("day")\
                             ,F.month(F.col("order_date")).alias("month")\
                             ,F.year(F.col("order_date")).alias("year"))
    dates_df.write.partitionBy('year').parquet(f"/dshop_datalake/silver/dates/{date}", mode='overwrite')
    logging.info(f"The table 'dates' for the date {date} loaded to silver successfully")

def loadProductsTableToSilver(spark, date):
    products_df = spark.read.load(f"/dshop_datalake/bronze/products/products_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    departments_df = spark.read.load(f"/dshop_datalake/bronze/departments/departments_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    aisles_df = spark.read.load(f"/dshop_datalake/bronze/aisles/aisles_{date}.csv",\
                                    header="true",inferSchema="true",format="csv")
    products_df = products_df.join(departments_df, ['department_id']).drop('department_id').join(aisles_df, ['aisle_id']).drop('aisle_id')
    products_df = products_df.where(F.col("product_id").isNotNull())\
        .where(F.col("product_name").isNotNull()).where(F.col("department").isNotNull())\
        .where(F.col("aisle").isNotNull())
    products_df = products_df.dropDuplicates()
    products_df.write.partitionBy('aisle').parquet(f"/dshop_datalake/silver/products/{date}", mode='overwrite')
    logging.info(f"The table 'products' for the date {date} loaded to silver successfully")

def loadAPIDataToSilver(spark, date):
    api_df = spark.read.json(f"/api_datalake/bronze/{date}.json")
    api_df = api_df.where(F.col("date").isNotNull())\
        .where(F.col("product_id").isNotNull())
    api_df = api_df.dropDuplicates()
    api_df.write.parquet(f"/api_datalake/silver/{date}", mode='overwrite')
    logging.info(f"The data from API for the date {date} loaded to bronze successfully")
