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
		self.fields = ('first_name', 'last_name', 'email', 'phone', 'registration_date', 'loyalty_status',)
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
		self._actions = {
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
		}
		self._phones = {
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
		}
		self._devices = {
			'Windows',
			'Linux',
			'MacOS'
		}
		self._pages = {
			'/catalog',
			'/product',
			'/cart',
			'/pay',
			'/checkout',
			'/account',
			'/account/orders',
			'/search',
			'/info/delivery'
		}
		super().__init__()
	
	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def start_time_gen_func(self):
		return self._fake.date_time_between(start_date=date(2022, 1, 1), end_date=date(2023, 1, 1))

	def pages_visited_gen_fun(self):
		return random.sample(self._actions, random.randint(2, 6))

	def device_gen_fun(self):
		if random.randint(0, 1):
			return random.sample(self._devices, 1)
		return random.sample(self._phones, 1) + ' ' + str(random.randint(5, 10))

	def actions_gen_fun(self):
		return random.sample(self._actions, random.randint(2, 6))

	def _after_gen(self, result, *args, **kwargs):
		for item in result:
			item['end_time'] += timedelta(minutes=random.randint(1, 30))

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
			self._product_ids.remove(product_id) # мутировать плохо! но мы аккуратно
			return product_id 

		raise Exception('Товаров нет')

	def price_changes_gen_func(self):
		start_date = dt.now() - timedelta(365) 

		return [{
			'price' : i + random.randint(100, 1000),
			'date' : start_date -  timedelta(18 * i)
		} for i in range(1, random.randint(5, 20))]

	def current_price_gen_func(self):
		return random.randint(1, 10000)

	def currency_gen_func(self):
		return 'RUB'

# SupportTickets, обращения в поддержку:
# ·	ticket_id — уникальный идентификатор тикета;
# ·	user_id — идентификатор пользователя;
# ·	status — статус тикета;
# ·	issue_type — тип проблемы;
# ·	messages — массив сообщений;
# ·	created_at — время создания тикета;
# ·	updated_at — время последнего обновления.


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
		return random.choices(self._user_id)

	def status_gen_func(self):
		return random.choices(self._statuses)

	def issue_type_gen_func(self):
		return random.randint(1, 10000)

	def messages_at_gen_func(self):
		return [ self._fake.text(50) for _ in range(1, 20)]

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

class Order(BaseFakeGenClass):
	def __init__(self, user_ids) -> None:
		self.fields = ('user_id',  'order_date', 'total_amount', 'status', 'delivery_date',)
		self._user_ids = user_ids
		self.model_name = 'Orders'
		self.colum_id = 'order_id'
		super().__init__()
  
	def _after_gen(self, result, *args, **kwargs):
		for item in result:
			if item['status'] == 'Completed':
				item['delivery_date'] = item['order_date'] + timedelta(days=random.randint(3, 6))
			else:
				item['delivery_date'] = dt.now().date() + timedelta(days=random.randint(6, 8))

		return result

	def user_id_gen_func(self):
		return random.choice(self._user_ids)

	def order_date_gen_func(self):
		return self._fake.date_time_between(start_date=date(2023, 7, 1), end_date=date(2025, 1, 1))

	def total_amount_gen_func(self):
		return 0 # его надо пересчитывать уже на готовом заказе

	def status_gen_func(self):
		return random.choice(('Paid', 'Delivery', 'Completed',))

	def delivery_date_gen_func(self):
		return date(2024, 1, 1) # зависит от параметра статус заказ, генерируется позже


class OrderDetails(BaseFakeGenClass):
	def __init__(self, order_ids, product_data) -> None:
		self.fields = ('order_id',  'product_id', 'quantity', 'price_per_unit', 'total_price', )
		self._order_ids = order_ids
		self._product_data = product_data
		self._product_ids = [key for key in product_data]
		super().__init__()
		self.model_name = 'OrderDetails'
		self.colum_id = 'order_detail_id'
	
	def _after_gen(self, result, *args, **kwargs):
		for item in result:
			product_info = self._product_data[item['product_id']]
			item['price_per_unit'] = product_info['price']
			item['total_price'] = item['price_per_unit'] * item['quantity']

		return result

	def order_id_gen_func(self):
		return random.choice(self._order_ids)

	def product_id_gen_func(self):
		return random.choice(self._product_ids)

	def quantity_gen_func(self):
		return random.randint(1, 5)

	def price_per_unit_gen_func(self):
		return 0 # рассчитывается позже

	def total_price_gen_func(self):
		return 0 # рассчитывается позже
