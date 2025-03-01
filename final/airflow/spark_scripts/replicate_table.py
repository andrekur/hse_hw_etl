import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Указываем схему вручную
schema = StructType([
	StructField("_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
	StructField("email", StringType(), True),
	StructField("registration_date", DateType(), True)
])

# TODO вынести в общую функцию репликации
def replicate(from_db, to_db):
	spark = SparkSession.builder \
		.appName('ReplicateData') \
		.config('spark.jars', '/opt/airflow/spark/jars/mongo-spark-connector_2.12-3.0.1-assembly.jar,\
			/opt/airflow/spark/jars/postgresql-42.2.18.jar') \
		.config("spark.mongodb.input.uri", "mongodb://root:example@db_mongo:27017/shop.Users?authSource=admin") \
    	.config("spark.mongodb.input.sampleSize", 100000) \
		.getOrCreate()

	df = spark.read \
		.format('mongo') \
		.load()

		# .option('database', 'shop') \
		# .option('collection', from_db.table) \

	df.show()
	df = df.withColumn("_id", col("_id.oid"))
	df.write \
		.format('jdbc') \
		.option('url', to_db.conn_url) \
		.option('dbtable', to_db.table) \
		.option('user', to_db.user['login']) \
		.option('password', to_db.user['passwd']) \
		.option('driver', to_db.driver) \
		.mode('overwrite') \
		.save()

	spark.stop()


if __name__ == "__main__":
	replicate_table = sys.argv[1] # table name get from args

	postgres_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"{replicate_table}"',
		'public'
	)

	mongo_config = ConnectionConfig(
		{'login': 'root', 'passwd': 'example'},
		'mongo',
		'db_mongo',
		'27017',
		'shop',
		replicate_table,
	)

	replicate(mongo_config, postgres_config)