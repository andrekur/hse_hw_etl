from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import to_json, struct, col
from datetime import datetime


schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("amount", IntegerType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("is_fraud", BooleanType(), False)
])


def main():
   spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

   test_data = [
      Row(
         transaction_id=1,
         user_id=1001,
         amount=5000,
         currency="USD",
         transaction_date=datetime(2025, 6, 15, 12, 0, 0),
         is_fraud=False
      ),
      Row(
         transaction_id=2,
         user_id=1002,
         amount=7500,
         currency="EUR",
         transaction_date=datetime(2025, 6, 15, 12, 5, 0),
         is_fraud=True
      )
   ]

   df = spark.createDataFrame(test_data, schema=schema)

   df_json = df.select(
      to_json(
            struct([
               col("transaction_id"),
               col("user_id"),
               col("amount"),
               col("currency"),
               col("transaction_date"),
               col("is_fraud")
            ])
      ).alias("value")
   )
   df_json.write.format("kafka") \
      .option("kafka.bootstrap.servers", "rc1a-5fqvq8nme5euhmt8.mdb.yandexcloud.net:9091") \
      .option("topic", "dataproc-kafka-topic") \
      .option("kafka.security.protocol", "SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
      .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username=" # так тоже лучше не делать)
              "password="
              ";") \
      .save()

if __name__ == "__main__":
   main()
