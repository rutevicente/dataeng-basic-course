import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def save_parquet(df, batch_id):
  (df
    .withColumn("batch_id",F.lit(batch_id))
    .withColumn("load_time",F.current_timestamp())
    .write.mode("append")
    .parquet("gs://edit-data-eng-dev/datalake/bronze/rate/data")
    )

# define spark session
spark = SparkSession.builder.appName('Test streaming').getOrCreate()

# read stream
stream1 = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# write stream as parquet with foreachBatch
query = (stream1.writeStream
    .option('checkpointLocation', 'gs://edit-data-eng-dev/datalake/bronze/rate/checkpoint')
    .trigger(processingTime='20 seconds')
    .outputMode('append')
    .foreachBatch(save_parquet)
    .start()
    .awaitTermination(120)
    )
