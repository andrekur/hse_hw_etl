import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


# TODO вынести в общую функцию репликации
def replicate(from_db, to_db):


	df = spark.read \
		.format('mongo') \
		.load()
	
	#python ./scripts/replicate_table.py Products Users Products 
	#python ./scripts/replicate_table.py Users Users Products  ProductPriceHistory
	#python ./scripts/replicate_table.py ProductPriceHistory
	#python ./scripts/replicate_table.py UserSessions
	# df.printSchema()
	df = df.withColumn("_id", col("_id.oid"))
	# df.printSchema()
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

def replica_Users(df):
	df = spark.read \
		.format('mongo') \
		.load()
	
	df = df.withColumn("_id", col("_id.oid"))

def replica_UserSessions(df):
	df = df.withColumn("_id", col("_id.oid"))
	df = df.withColumn("user_id", col("user_id.oid"))

	df = df.withColumn("pages_visited", to_json(col("pages_visited")))
	df = df.withColumn("actions", to_json(col("actions")))

	df.printSchema()
	return df

def replica_Products(df):
	df = df.withColumn("_id", col("_id.oid"))


def replica_ProductPriceHistory(df):
	df = df.withColumn("_id", col("_id.oid"))
	df = df.withColumn("product_id", col("product_id.oid"))

	df = df.withColumn("price_changes_json", to_json(col("price_changes")))
	df = df.drop("price_changes").withColumnRenamed("price_changes_json", "price_changes")


def replica_SupportTickets(df):
	pass

def replica_UserRecommendations(df):
	pass

def replica_SearchQueries(df):
	pass

def replica_EventLogs(df):
	pass

def replica_ModerationQueue(df):
	pass

tables_replica_func = {
	'Users': replica_Users,
	'UserSessions': replica_UserSessions,
	'Products': replica_Products,
	'ProductPriceHistory': replica_ProductPriceHistory,
	'SupportTickets': replica_SupportTickets,
	'UserRecommendations': replica_UserRecommendations,
	'SearchQueries': replica_SearchQueries,
	'EventLogs': replica_EventLogs,
	'ModerationQueue': replica_ModerationQueue
}



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

	spark = SparkSession.builder \
		.appName('ReplicateData') \
		.config('spark.jars', '/opt/airflow/spark/jars/mongo-spark-connector_2.12-3.0.1-assembly.jar,\
			/opt/airflow/spark/jars/postgresql-42.2.18.jar') \
		.config("spark.mongodb.input.uri", f"mongodb://root:example@db_mongo:27017/shop.{replicate_table}?authSource=admin") \
		.config("spark.mongodb.input.sampleSize", 100000) \
		.getOrCreate()

	df = spark.read \
		.format('mongo') \
		.load()

	# replicate(mongo_config, postgres_config)
	df = tables_replica_func[replicate_table](df)

	df.write \
		.format('jdbc') \
		.option('url', postgres_config.conn_url) \
		.option('dbtable', postgres_config.table) \
		.option('user', postgres_config.user['login']) \
		.option('password', postgres_config.user['passwd']) \
		.option('driver', postgres_config.driver) \
		.mode('overwrite') \
		.save()

	spark.stop()

