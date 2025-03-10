class ConnectionConfig:
	def __init__(self, user, db_type, host, port, db_name, table_name=None, schema=None) -> None:
		db_drivers = {
			'postgresql': 'org.postgresql.Driver',
		}

		self.user = user
		self.driver = db_drivers.get(db_type)
		self.conn_url = f'jdbc:{db_type}://{host}:{port}/{db_name}'

		if table_name:
			self.table = f'{schema}.{table_name}' if schema else table_name
