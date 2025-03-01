import random
from datetime import date
from datetime import timedelta
from datetime import datetime as dt

from faker import Faker
from faker_commerce import Provider


class BaseFakeGenClass:
	def __init__(self) -> None:
		self._fake = Faker()
		self._fake.add_provider(Provider)

	def _prepared_gen(self, *args, **kwargs):
		return []

	def _after_gen(self, result, *args, **kwargs):
		return result

	def gen_data(self, count=0):
		result = self._prepared_gen()

		for _ in range(count):
			result.append({
				field: getattr(self, f'{field}_gen_func')() if getattr(self, f'{field}_gen_func') else None
				for field in self.fields
			})

		return self._after_gen(result)
  
	def insert_in_db(self, cur, data):
		# TODO
		# не совсем безопасно, но мы аккуратно
		result_ids = []
		for i in range(0, len(data), 1000):
			end = i + 1000 if i + 1000 + 1 < len(data) else len(data) - 1
			batch = data[i:end]

			data_to_insert = ''
			for item in batch:
				_columns = ','.join(str(val) for val in self.fields)

				_insert_data = ''
				for key in self.fields:
					val = item[key]
					if type(val) is str:
						_insert_data += f"'{val}',"
					elif type(val) is dt or type(val) is date:
						_insert_data += f"'{val.strftime('%Y-%m-%d')}',"
					else:
						_insert_data += f'{val},'
				_insert_data = f'({_insert_data[:-1]}),'
				data_to_insert +=_insert_data
			data_to_insert = data_to_insert[:-1]

			print(f"INSERT INTO '{self.model_name}' ({_columns}) VALUES {data_to_insert}")
			cur.execute(
				f"INSERT INTO '{self.model_name}' ({_columns}) VALUES {data_to_insert}"
				f"RETURNING {self.colum_id}"
			)
			result_ids += [val[0] for val in cur.fetchall()]
		print(result_ids)
		return result_ids

class User(BaseFakeGenClass):
	def __init__(self) -> None:
		self.fields = ('first_name', 'last_name', 'email', 'phone', 'registration_date', )
		self.model_name = 'Users'
		self.colum_id = 'user_id'
		super().__init__()

	def first_name_gen_func(self):
		return self._fake.first_name()

	def last_name_gen_func(self):
		return self._fake.last_name()

	def email_gen_func(self):
		return self._fake.email()

	def phone_gen_func(self):
		return self._fake.phone_number()[:10]

	def registration_date_gen_func(self):
		return self._fake.date_time_between(start_date=date(2022, 1, 1), end_date=date(2023, 1, 1))

class UserSession(BaseFakeGenClass):
	def __init__(self, user_ids) -> None:
		self.fields = ('user_id', 'start_time', 'end_time', 'pages_visited', 'device', 'actions',)
		self.model_name = 'UserSession'
		self.colum_id = 'session_id'
		self._user_ids = user_ids
		self._actions = (
			'view_product_details',
			'add_to_cart',
			'remove_from_cart',
			'apply_discount_code',
			'proceed_to_checkout',
			'select_shipping_method',
			'make_payment',
			'write_product_review',
			'track_order_status',
			'contact_customer_support'
		)
		self._phones = (
			'Apple',
			'Samsung',
			'Xiaomi',
			'Huawei',
			'Oppo',
			'Vivo',
			'OnePlus',
			'Google',
			'Sony',
			'Nokia'
		)
		self._devices = (
			'Windows',
			'Linux',
			'MacOS'
		)
		self._pages = (
			'/catalog',
			'/product',
			'/cart',
			'/pay',
			'/checkout',
			'/account',
			'/account/orders',
			'/search',
			'/info/delivery'
		)
		super().__init__()
	
	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def start_time_gen_func(self):
		return self._fake.date_time_between(start_date=date(2022, 1, 1), end_date=date(2023, 1, 1))
	
	def end_time_gen_func(self):
		return None

	def pages_visited_gen_func(self):
		return random.sample(self._pages, random.randint(2, 6))

	def device_gen_func(self):
		if random.randint(0, 1):
			return random.choice(self._devices)
		return random.choice(self._phones) + ' ' + str(random.randint(5, 10))

	def actions_gen_func(self):
		return random.sample(self._actions, random.randint(2, 6))

	def _after_gen(self, result, *args, **kwargs):
		for item in result:
			item['end_time'] = item['start_time'] + timedelta(minutes=random.randint(1, 30))

		return result

class Product(BaseFakeGenClass):
	def __init__(self) -> None:
		self.fields = ('name', 'description', 'stock_quantity', 'creation_date',)
		self.model_name = 'Products'
		self.colum_id = 'product_id'
		super().__init__()

	def name_gen_func(self):
		return self._fake.words(nb=1)[0]

	def description_gen_func(self):
		return self._fake.text(500)

	def stock_quantity_gen_func(self):
		return random.randint(1, 10000)

	def creation_date_gen_func(self):
		return self._fake.date_time_between(start_date=date(2023, 1, 1), end_date=date(2023, 6, 10))


class ProductPriceHistory(BaseFakeGenClass):
	def __init__(self, product_ids) -> None:
		self.fields = ('product_id', 'price_changes', 'current_price', 'currency',)
		self.model_name = 'ProductPriceHistory'
		self.colum_id = 'product_history_id'
		self._product_ids = product_ids
		super().__init__()

	def product_id_gen_func(self):
		if (self._product_ids):
			product_id = random.choice(self._product_ids)
			self._product_ids.remove(product_id) # плохо! но мы аккуратно
			return product_id 

		raise Exception('Товаров нет')

	def price_changes_gen_func(self):
		start_date = dt.now() - timedelta(365) 

		return [{
			'price' : i + random.randint(100, 1000),
			'date' : start_date + timedelta(18 * i)
		} for i in range(1, random.randint(5, 20))]

	def current_price_gen_func(self):
		return random.randint(1, 10000)

	def currency_gen_func(self):
		return 'RUB'


class SupportTickets(BaseFakeGenClass):
	def __init__(self, user_ids) -> None:
		self.fields = ('user_id', 'status', 'issue_type', 'messages', 'created_at', 'updated_at', 'end_date')
		self.model_name = 'SupportTickets'
		self.colum_id = 'ticket_id'
		self._user_ids = user_ids
		self._statuses = ['New', 'InWork', 'Closed']
		self._types = [
			'delivery_issue',
			'missing_item',
			'damaged_product',
			'order_error',
			'return_inquiry',
			'promo_code_issue',
			'order_cancellation',
			'warranty_question',
			'payment_problem',
    		'product_consultation'
		]
		super().__init__()

	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def status_gen_func(self):
		return random.choice(self._statuses)

	def issue_type_gen_func(self):
		return random.choice(self._types)

	def messages_gen_func(self):
		return [self._fake.text(50) for _ in range(1, 20)]

	def created_at_gen_func(self):
		return dt.now() - timedelta(365) + timedelta(random.randint(1, 15))

	def end_date_gen_func(self):
		return None

	def updated_at_gen_func(self):
		return None

	def _after_gen(self, result, *args, **kwargs):
		for item in result:
			if item['status'] == 'Closed':
				item['end_date'] = item['created_at'] + timedelta(days=random.randint(3, 6))

			item['updated_at'] = item['created_at'] + timedelta(2)

		return result


class UserRecommendations(BaseFakeGenClass):
	def __init__(self, user_ids, product_ids) -> None:
		self.fields = ('recommended_products', 'last_updated',)
		self.model_name = 'UserRecommendations'
		self.colum_id = 'recom_id'
		self._user_ids = user_ids
		self._product_ids = product_ids
		super().__init__()

	def recommended_products_gen_func(self):
		return random.sample(self._product_ids, 10)

	def last_updated_gen_func(self):
		return dt.now() - timedelta(180) + timedelta(random.randint(1, 15))


class SearchQueries(BaseFakeGenClass):
	def __init__(self, user_ids, product_names) -> None:
		self.fields = ('user_id', 'query_text', 'timestamp', 'filters', 'results_count', )
		self.model_name = 'SearchQueries'
		self.colum_id = 'query_id'
		self._user_ids = user_ids
		self._product_names = product_names
		self._filters = [
			'low_price',
			'with_feedback',
			'max_rating',
			'discount',
			'shipping_options'
		]
		super().__init__()

	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def query_text_gen_func(self):
		return self._fake.text(20) + random.choice(self._product_names)[:10]

	def last_updated_gen_func(self):
		return dt.now() - timedelta(180) + timedelta(random.randint(1, 15))

	def timestamp_gen_func(self):
		return random.randint(30, 3000) # ms

	def filters_gen_func(self):
		return random.sample(self._filters, 2)

	def results_count_gen_func(self):
		return random.randint(5, len(self._product_names))


class EventLogs(BaseFakeGenClass):
	def __init__(self, user_ids) -> None:
		self.fields = ('timestamp', 'event_type', 'details',)
		self.model_name = 'SearchQueries'
		self.colum_id = 'log_id'
		self._user_ids = user_ids
		self._server_event_types = [
			'server_error',
			'server_overload',
			'server_database_connection_error',
			'server_security_alert',
			'server_api_rate_limit_exceeded',
			'server_backup_failed',
			'server_cron_job_failed',
			'server_health_check'
		]
		super().__init__()

	def timestamp_gen_func(self):
		return dt.now() - timedelta(180) + timedelta(random.randint(1, 180))

	def event_type_gen_func(self):
		return random.sample(self._server_event_types, 1)

	def details_gen_func(self):
		return {
			'user': random.choice(self._user_ids),
			'description': self._fake.text(30)
		}


class ModerationQueue(BaseFakeGenClass):
	def __init__(self, user_ids, product_ids) -> None:
		self.fields = ('user_id', 'product_id', 'review_text', 'rating', 'moderation_status', 'submitted_at', 'review_end')
		self.model_name = 'SearchQueries'
		self.colum_id = 'review_id'
		self._user_ids = user_ids
		self._product_ids = product_ids
		self._statuses = ['New', 'InWork', 'Success', 'Broken']

		super().__init__()

	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def product_id_gen_func(self):
		return random.choice(self._user_ids)

	def review_text_gen_func(self):
		return self._fake.text(random.randint(100, 200))

	def rating_gen_func(self):
		return random.choice([1, 2, 3, 4, 5])
	
	def moderation_status_gen_func(self):
		return random.choice(self._statuses)
	
	def submitted_at_gen_func(self):
		return dt.now() - timedelta(180) + timedelta(random.randint(1, 180))
	
	def review_end_gen_func(self):
		return None

	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def _after_gen(self, result, *args, **kwargs):
		user_product = set()
		new_result = []

		for item in result:
			if item['moderation_status'] in ('Success', 'Broken'):
				item['review_end'] = item['submitted_at'] + timedelta(hours=random.randint(1, 6))

			val = str(item['user_id']) + '_' + str(item['product_id'])
			if val not in user_product:
				user_product.add(val)
				new_result.append(item)

		return new_result
