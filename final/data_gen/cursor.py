from contextlib import contextmanager

import psycopg2
from psycopg2.pool import SimpleConnectionPool

class DatabaseCursor:
	def __init__(self, *args, **kwargs):
		self.connection_pool = SimpleConnectionPool(*args, **kwargs)

	@contextmanager
	def _get_conn(self):
		conn = self.connection_pool.getconn()
		try:
			yield conn
		finally:
			self.connection_pool.putconn(conn)
