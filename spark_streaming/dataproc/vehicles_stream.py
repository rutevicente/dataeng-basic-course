from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def insert_vehicles(df, batch_id):
    bronze_path=f"gs://edit-data-eng-dev/datalake/bronze/vehicles"
    df.write.format("parquet").mode("append").save(bronze_path)
  
if __name__ == '__main__':

    # Getting spark session
    spark = SparkSession.builder.appName('Test streaming').getOrCreate()

    vehicle_schema = StructType([StructField('bearing', IntegerType(), True),
                                 StructField('block_id', StringType(), True),
                                 StructField('current_status', StringType(), True),
                                 StructField('id', StringType(), True),
                                 StructField('lat', FloatType(), True),
                                 StructField('line_id', StringType(), True),
                                 StructField('lon', FloatType(), True),
                                 StructField('pattern_id', StringType(), True),
                                 StructField('route_id', StringType(), True),
                                 StructField('schedule_relationship', StringType(), True),
                                 StructField('shift_id', StringType(), True),
                                 StructField('speed', FloatType(), True),
                                 StructField('stop_id', StringType(), True),
                                 StructField('timestamp', TimestampType(), True),
                                 StructField('trip_id', StringType(), True)])

    # define paths
    bucket_name="edit-data-eng-dev"
    table_path="vehicles"
    landing_path=f"gs://{bucket_name}/datalake/landing/{table_path}"
    bronze_path=f"gs://{bucket_name}/datalake/bronze/{table_path}"

    stream = spark.readStream.format("json").schema(vehicle_schema).load(landing_path)

    query = (stream
        .writeStream
        .outputMode("append")
        .foreachBatch(insert_vehicles)
        .option("checkpointLocation", "/content/bronze/checkpoint")
        .trigger(processingTime='20 seconds')
        .start()
        .awaitTermination(30)
        )
    
    print("Process done")