from pyspark.sql import SparkSession
import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import SQLTransformer


os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_PYTHON'] = sys.executable

############################################################################
# global var.
want_to_publish_data_on_kafak = True
file_path = '/home/chandreshjsutariya/Train_details_22122017.csv'
rows_to_send_count = 0
rows_to_send = 2
count=0
# Define Kafka producer parameters
kafka_bootstrap_servers = "35.231.64.61:9092"
kafka_topic = "test-2"
###########################################################################

spark = (
    SparkSession.builder
        .master("local")
        .appName("ass-8")
        .getOrCreate()
)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

trains = spark.read.format('csv').load(file_path, header=True)
trains.show()


remove_null = SQLTransformer(statement="SELECT * FROM __THIS__ \
                                        WHERE 'Arrival Time' IS NOT NULL AND 'Departure Time' IS NOT NULL")
trains = remove_null.transform(trains)

trains.count()
#186124

from pyspark.sql.functions import col, expr
# from kafka import KafkaProducer
import json

trains.count()

trains = trains.filter((col("Arrival time") != "00:00:00") & \
                       (col("Departure Time") != "00:00:00"))
trains.count()

trains = trains.withColumn("AT", expr(\
   "to_timestamp(`Arrival time`, 'HH:mm:ss')"\
    )
    )
trains = trains.withColumn("DT", expr(\
   "to_timestamp(`Departure Time`, 'HH:mm:ss')"\
    )
    )
trains.count()

trains = trains.withColumn("TimeOnPltf",
                   expr("CASE WHEN `DT` >= `AT` " +
                        "THEN (unix_timestamp(`DT`) - unix_timestamp(`AT`)) / 60 " +
                        "ELSE (86400 - unix_timestamp(`AT`)) / 60 + (unix_timestamp(`DT`) / 60) END")
)
# trains.show()

# drop rows with null values in platformstaymin col
trains = trains.dropna(subset=["TimeOnPltf"])
trains.count()

# function to send messge to kafka unsin kafkaproducer
from kafka import KafkaProducer

def send_to_kafka_partition(iter):
  producer = KafkaProducer(bootstrap_servers = kafka_bootstrap_servers,
                           value_serializer = lambda x: json.dumps(x).encode('utf-8'))
  for row in iter:
    message = {
        "StationCode": row['Station Code'],
        "AT": row['Arrival time'],
        "DT": row['Departure Time'],
        "TimeOnPltf": row['TimeOnPltf']
    }
    producer.send(kafka_topic, value=message)
    
  producer.flush()

if want_to_publish_data_on_kafak:
    trains.foreachPartition(send_to_kafka_partition)

spark.stop()