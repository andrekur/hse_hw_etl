#!/usr/bin/env python3

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct

def main():
   spark = SparkSession.builder \
      .appName("dataproc-kafka-read-batch-app") \
      .getOrCreate()

   df = spark.readStream.format("kafka") \
      .option("kafka.bootstrap.servers", "rc1a-5fqvq8nme5euhmt8.mdb.yandexcloud.net:9091") \
      .option("subscribe", "dataproc-kafka-topic") \
      .option("kafka.security.protocol", "SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
      .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username="
              "password="
              ";") \
      .option("startingOffsets", "earliest") \
      .load() \
      .selectExpr("CAST(value AS STRING)") \
      .where(col("value").isNotNull())

   query = df.writeStream \
      .foreachBatch(lambda batch_df, batch_id:
         batch_df.write \
            .mode("append") \
            .format("text") \
            .save("s3a://mentor-hw/etl_final/task_3/result")
      ) \
      .trigger(once=True) \
      .start()

   query.awaitTermination()

if __name__ == "__main__":
   main()
