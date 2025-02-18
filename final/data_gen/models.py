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
						_insert_data += f''{val}','
					elif type(val) is dt or type(val) is date:
						_insert_data += f''{val.strftime('%Y-%m-%d')}','
					else:
						_insert_data += f'{val},'
				_insert_data = f'({_insert_data[:-1]}),'
				data_to_insert +=_insert_data
			data_to_insert = data_to_insert[:-1]

			print(f'INSERT INTO '{self.model_name}' ({_columns}) VALUES {data_to_insert}')
			cur.execute(
				f'INSERT INTO '{self.model_name}' ({_columns}) VALUES {data_to_insert}'
				f'RETURNING {self.colum_id}'
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
		self.model_name = 'UserSessions'
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
# UserSessions, сессии пользователей:
# ·	session_id — уникальный идентификатор сессии;
# ·	user_id — идентификатор пользователя;
# ·	start_time — время начала сессии;
# ·	end_time — время завершения сессии;
# ·	pages_visited — массив посещённых страниц;
# ·	device — информация об устройстве;
# ·	actions — массив действий пользователя.



class ProductCategories(BaseFakeGenClass):
	def __init__(self) -> None:
		self.fields = ('name',)
		self.model_name = 'ProductCategories'
		self.colum_id = 'category_id'
		super().__init__()

	def _prepared_gen(self):
		_base_categories = [
			'Home Appliances',
			'Fashion',
			'Beauty Products',
			'Sports Gear',
			'Electronics',
			'Books',
			'Toys',
			'Automotive Accessories',
			'Gardening Supplies',
			'Health'
		]

		return [{'name': category} for category in _base_categories]

class ProductCategoriesWithParent(BaseFakeGenClass):
	def __init__(self, categories_ids) -> None:
		self.fields = ('name', 'parent_category_id')
		self._categories_ids = categories_ids
		self.model_name = 'ProductCategories'
		self.colum_id = 'category_id'
		super().__init__()

	def name_gen_func(self):
		return self._fake.words(nb=1)[0]

	def parent_category_id_gen_func(self):
		return random.choice(self._categories_ids)

class Product(BaseFakeGenClass):
	def __init__(self, categories_ids) -> None:
		self.fields = ('name', 'description', 'category_id', 'price', 'stock_quantity', 'creation_date',)
		self._categories_ids = categories_ids
		self.model_name = 'Products'
		self.colum_id = 'product_id'
		super().__init__()

	def name_gen_func(self):
		return self._fake.words(nb=1)[0]

	def description_gen_func(self):
		return self._fake.text(500)

	def category_id_gen_func(self):
		return random.choice(self._categories_ids)

	def price_gen_func(self):
		return round(random.uniform(10.0, 10000.0), 2)

	def stock_quantity_gen_func(self):
		return random.randint(1, 10000)

	def creation_date_gen_func(self):
		return self._fake.date_time_between(start_date=date(2023, 1, 1), end_date=date(2023, 6, 10))


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
