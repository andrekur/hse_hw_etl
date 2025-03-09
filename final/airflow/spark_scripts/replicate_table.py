import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, expr
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def replica_Users(df):
	df = df.withColumn('_id', col('_id.oid'))

	return df

def replica_UserSessions(df):
	df = df.withColumn('_id', col('_id.oid'))
	df = df.withColumn('user_id', col('user_id.oid'))

	df = df.withColumn('pages_visited', to_json(col('pages_visited')))
	df = df.withColumn('actions', to_json(col('actions')))

	return df

def replica_Products(df):
	df = df.withColumn('_id', col('_id.oid'))

	return df

def replica_ProductPriceHistory(df):
	df = df.withColumn('_id', col('_id.oid'))
	df = df.withColumn('product_id', col('product_id.oid'))

	df = df.withColumn('price_changes', to_json(col('price_changes')))

	return df

def replica_SupportTickets(df):

	df = df.withColumn('_id', col('_id.oid'))
	df = df.withColumn('messages', to_json(col('messages')))
	df = df.withColumn('user_id', col('user_id.oid'))

	return df

def replica_UserRecommendations(df):
	
	df = df.withColumn('_id', col('_id.oid'))

	df = df.withColumn(
		'recommended_products',
		expr('transform(recommended_products, x -> x.oid)')
	)

	return df

def replica_SearchQueries(df):

	df = df.withColumn('_id', col('_id.oid'))
	df = df.withColumn('user_id', col('user_id.oid'))

	df = df.withColumn('filters', to_json(col('filters')))

	return df

def replica_EventLogs(df):

	df = df.withColumn('_id', col('_id.oid'))
	df = df.withColumn('details',
    	struct(
        	col('details.user.oid').alias('user_id'),
        	col('details.description')
    )
	)

	df = df.withColumn('details', to_json(col('details')))

	return df

def replica_ModerationQueue(df):

	df = df.withColumn('_id', col('_id.oid'))
	df = df.withColumn('user_id', col('user_id.oid'))
	df = df.withColumn('product_id', col('product_id.oid'))

	return df

TABLES_REPLICA_FUNC = {
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

MONGO_URI = f"mongodb://{CONFIG['DB_MONGO_USER']}:{CONFIG['DB_MONGO_PASSWORD']}@{CONFIG['DB_MONGO_HOST']}:{CONFIG['DB_MONGO_PORT']}/"
DB_MONGO_NAME_DB = CONFIG['DB_MONGO_NAME_DB']

if __name__ == '__main__':
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
		.config('spark.jars',
			'/opt/airflow/spark/jars/mongo-spark-connector_2.12-3.0.1-assembly.jar,'
			'/opt/airflow/spark/jars/postgresql-42.2.18.jar'
		) \
		.config('spark.mongodb.input.uri', f'{MONGO_URI}{DB_MONGO_NAME_DB}.{replicate_table}?authSource=admin') \
		.config('spark.mongodb.input.sampleSize', 100000) \
		.getOrCreate()

	df = spark.read \
		.format('mongo') \
		.load()

	df = TABLES_REPLICA_FUNC[replicate_table](df)

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
