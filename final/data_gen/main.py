from models import (
  User, ProductCategories, ProductCategoriesWithParent, Product, Order, OrderDetails
)
from cursor import DatabaseCursor

from dotenv import dotenv_values


CONFIG = dotenv_values('./.env')

CONNECTION = DatabaseCursor(
	2,
	10,
	user=CONFIG['DB_POSTGRES_USER'],
	password=CONFIG['DB_POSTGRES_PASSWORD'],
	host=CONFIG['DB_POSTGRES_HOST'],
	port=CONFIG['DB_POSTGRES_PORT'],
	database=CONFIG['DB_POSTGRES_NAME_DB']
)

COUNT_USERS = int(CONFIG['DATA_GEN_COUNT_USERS'])
COUNT_PRODUCTS = int(CONFIG['DATA_GEN_COUNT_PRODUCTS'])
CATEGORIES_WITH_PARENTS = int(CONFIG['DATA_GEN_COUNT_CATEGORIES_WITH_PARENTS'])
COUNT_ORDERS = int(CONFIG['DATA_GEN_COUNT_ORDERS'])
ORDER_DETAILS = int(CONFIG['DATA_GEN_COUNT_ORDER_DETAILS'])


if __name__ == '__main__':
	with (
		CONNECTION._get_conn() as conn,
		conn.cursor() as cur
	):
		try:
			user = User()
			user_result = user.gen_data(COUNT_USERS)
			user_ids = user.insert_in_db(cur, user_result)

			pc = ProductCategories()
			pc_result = pc.gen_data()
			pc_ids = pc.insert_in_db(cur, pc_result)

			pcp = ProductCategoriesWithParent(pc_ids)
			pcp_result = pcp.gen_data(CATEGORIES_WITH_PARENTS)
			pcp_ids = pcp.insert_in_db(cur, pcp_result)

			products = Product([*pc_ids, *pcp_ids])
			products_result = products.gen_data(COUNT_PRODUCTS)
			products_ids = products.insert_in_db(cur, products_result)

			orders = Order(user_ids)
			orders_result = orders.gen_data(COUNT_ORDERS)
			orders_ids = orders.insert_in_db(cur, orders_result)

			prepared_product_data = {key:val for key, val in zip(products_ids, products_result )}
			od = OrderDetails(orders_ids, prepared_product_data)
			od_result = od.gen_data(ORDER_DETAILS)
			od_ids = od.insert_in_db(cur, od_result)

			conn.commit()
			cur.execute('select order_id, sum(total_price) from "OrderDetails" od group by order_id')
			for order_id, total_amount in cur.fetchall():
				cur.execute(f'UPDATE "Orders" set total_amount = {total_amount} where order_id={order_id}')
			conn.commit()
			cur.execute(f'DELETE FROM "Orders" where total_amount = 0')
			conn.commit()
		except Exception as e:
			conn.rollback()
			print(f'Error executing query: {e}')
