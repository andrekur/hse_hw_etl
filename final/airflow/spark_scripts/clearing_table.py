import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, expr
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def clearing_Users(df):
	return df

def clearing_UserSessions(df):
	return df

def clearing_Products(df):
	return df

def clearing_ProductPriceHistory(df):
	return df

def clearing_SupportTickets(df):
	return df

def clearing_UserRecommendations(df):
	return df

def clearing_SearchQueries(df):
	return df

def clearing_EventLogs(df):
	return df

def clearing_ModerationQueue(df):
	return df

TABLES_CLEARING_FUNC = {
	'Users': clearing_Users,
	'UserSessions': clearing_UserSessions,
	'Products': clearing_Products,
	'ProductPriceHistory': clearing_ProductPriceHistory,
	'SupportTickets': clearing_SupportTickets,
	'UserRecommendations': clearing_UserRecommendations,
	'SearchQueries': clearing_SearchQueries,
	'EventLogs': clearing_EventLogs,
	'ModerationQueue': clearing_ModerationQueue
}



if __name__ == "__main__":
    # TODO переделать на коннекшн к постгресу и перекладываение именно из него к него в клиар
	replicate_table = sys.argv[1] # table name get from args

	postgres_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"{replicate_table}"',
		'stage'
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

	df = TABLES_CLEARING_FUNC[replicate_table](df)

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

