from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,  DecimalType, TimestampType, BooleanType


spark = SparkSession.builder \
    .appName("create-table") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

input_path = "s3a://mentor-hw/etl_final/task_1/2025/06/14/transactions.csv.gz"
output_csv_path = "s3a://mentor-hw/etl_final/fraud_stats/2025/06/16/fraud_stats"


schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("amount", IntegerType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("is_fraud", BooleanType(), False)
])

df = spark.read.schema(schema).option("timestampFormat", "yyyy-MM-dd HH:mm:ss Z z").csv(input_path)

fraud_df = df.filter(col("is_fraud") == True)

sender_stats = fraud_df.groupBy("user_id") \
    .agg(
        count("*").alias("fraud_transactions_sent"),
        sum("amount").alias("total_fraud_amount_sent")
    )

sender_stats.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_csv_path)