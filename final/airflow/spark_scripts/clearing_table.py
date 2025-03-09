import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, substring, length, when, expr
from dotenv import dotenv_values

from db_conn_conf import ConnectionConfig

CONFIG = dotenv_values('.env')


def clearing_Users(df):
	# считаем, что телефон хранится с кодом страны
	# зануляем все не валидные телефоны, но сохраняем информацию о пользователях
	# для упрощения любой код страны считаем валидным
	df = df.withColumn('cleaned_phone', regexp_replace(col('phone'), '[^0-9]', ''))

	df = df.withColumn(
    	'cleaned_phone',
    	when(
			(length(col('cleaned_phone')) >= 11) & (length(col('cleaned_phone')) <= 14), col('cleaned_phone')
    	).otherwise('')
	)

	# выделяем код страны, через sql тк через спрак что-то идет не так)
	df = df.withColumn(
		'country_code',
    	expr(
			"CASE WHEN length(cleaned_phone) >= 11 THEN substring(cleaned_phone, 1, length(cleaned_phone) - 10) ELSE '' END"
		)
	)

	df = df.withColumn(
		'cleaned_phone',
    	expr(
			"CASE WHEN length(cleaned_phone) >= 11 THEN substring(cleaned_phone, length(country_code) + 1, length(cleaned_phone)) ELSE '' END"
		)
	)
	
	df = df.drop('phone')

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
	clearing_table = sys.argv[1] # table name get from args

	postgres_stage_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"{clearing_table}"',
		'stage'
	)

	postgres_cleaned_config = ConnectionConfig(
		{'login': CONFIG['DB_POSTGRES_USER'], 'passwd': CONFIG['DB_POSTGRES_PASSWORD']},
		'postgresql',
		CONFIG['DB_POSTGRES_HOST'],
		CONFIG['DB_POSTGRES_PORT'],
		CONFIG['DB_POSTGRES_NAME_DB'],
		f'"{clearing_table}"',
		'cleaned'
	)

	spark = SparkSession.builder \
		.appName('ReplicateData') \
		.config('spark.jars', '/opt/airflow/spark/jars/postgresql-42.2.18.jar') \
		.getOrCreate()

	df = spark.read \
		.format('jdbc') \
		.option('url', postgres_stage_config.conn_url) \
		.option('dbtable', postgres_stage_config.table) \
		.option('user', postgres_stage_config.user['login']) \
		.option('password', postgres_stage_config.user['passwd']) \
		.option('driver', postgres_stage_config.driver) \
		.load()

	df = TABLES_CLEARING_FUNC[clearing_table](df)

	df.write \
		.format('jdbc') \
		.option('url', postgres_cleaned_config.conn_url) \
		.option('dbtable', postgres_cleaned_config.table) \
		.option('user', postgres_cleaned_config.user['login']) \
		.option('password', postgres_cleaned_config.user['passwd']) \
		.option('driver', postgres_cleaned_config.driver) \
		.mode('overwrite') \
		.save()

	spark.stop()
