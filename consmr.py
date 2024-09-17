from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

spark = SparkSession\
    .builder\
        .appName("oppe-24")\
            .getOrCreate()

###############################################
# global var.
want_to_start_consumer = True
kafka_bootstrap_servers ='35.231.64.61:9092'
kafka_topic = 'test-2'
###############################################

###############################################
#setting up the kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets","earliest")\
    .load()\
    .selectExpr("CAST(value AS STRING) as value")
###############################################
if want_to_start_consumer:
    json_schema = "StationCode STRING, AT STRING, DT STRING, TrainName STRING, TimeOnPltf DOUBLE"

    parsed_df = df.selectExpr(\
                "from_json(value, '{}') as data"\
                    .format(json_schema)) \
                .select("data.*")\
                .filter(col("TimeOnPltf").isNotNull())

    parsed_df = parsed_df.\
        withColumn("AT", col("AT").cast("timestamp"))

    windowed_df = parsed_df.groupBy("StationCode", window("AT", "20 minutes", "20 minutes"))\
            .count()\
            .withColumnRenamed("count", "TrainsOnStation")


    query = windowed_df.writeStream\
            .outputMode("complete")\
            .format("console")\
            .option("truncate", "false")\
            .trigger(processingTime="5 seconds")\
            .query.start()

    query.awaitTermination()
spark.stop()