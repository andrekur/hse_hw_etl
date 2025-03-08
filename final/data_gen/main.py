from models import (
  User, UserSession, Product, ProductPriceHistory, SupportTickets, UserRecommendations, SearchQueries,
  EventLogs, ModerationQueue
)
from pymongo import MongoClient

from dotenv import dotenv_values

CONFIG = dotenv_values('./.env')

MONGO_URI = f"mongodb://{CONFIG['DB_MONGO_USER']}:{CONFIG['DB_MONGO_PASSWORD']}@{CONFIG['DB_MONGO_HOST']}:{CONFIG['DB_MONGO_PORT']}/"
DB_MONGO_NAME_DB = CONFIG['DB_MONGO_NAME_DB']

COUNT_USERS = int(CONFIG['DATA_GEN_COUNT_USERS'])
COUNT_USER_SESSIONS = int(CONFIG['DATA_GEN_COUNT_USER_SESSIONS'])
CATEGORIES_PRODUCTS = int(CONFIG['DATA_GEN_COUNT_PRODUCTS'])
COUNT_PRODUCT_PRICE_HISTORY = int(CONFIG['DATA_GEN_COUNT_PRODUCT_PRICE_HISTORY'])
COUNT_SUPPORTS_TICKETS = int(CONFIG['DATA_GEN_COUNT_SUPPORTS_TICKETS'])
COUNT_USER_RECOMMENDATIONS = int(CONFIG['DATA_GEN_COUNT_USER_RECOMMENDATIONS'])
COUNT_SEARCH_QUERIES = int(CONFIG['DATA_GEN_COUNT_SEARCH_QUERIES'])
COUNT_EVENT_LOGS = int(CONFIG['DATA_GEN_COUNT_EVENT_LOGS'])
COUNT_MODERATION_QUEUE = int(CONFIG['DATA_GEN_COUNT_MODERATION_QUEUE'])


if __name__ == '__main__':
	client = MongoClient(MONGO_URI)

	db = client[DB_MONGO_NAME_DB]

	user_collection = db['Users']
	user = User()
	user_result = user.gen_data(COUNT_USERS)
	user_ids = user_collection.insert_many(user_result).inserted_ids

	u_s_collection = db['UserSessions']
	user_session = UserSession(user_ids)
	session_result = user_session.gen_data(COUNT_USER_SESSIONS)
	session_ids = u_s_collection.insert_many(session_result)

	product_collection = db['Products']
	product = Product()
	product_result = product.gen_data(CATEGORIES_PRODUCTS)
	product_ids = product_collection.insert_many(product_result).inserted_ids

	product_p_h_collection = db['COUNT_PRODUCT_PRICE_HISTORY']
	product_p_h = ProductPriceHistory(product_ids.copy())
	product_p_h_result = product_p_h.gen_data(COUNT_PRODUCT_PRICE_HISTORY)
	product_p_h_ids = product_p_h_collection.insert_many(product_p_h_result).inserted_ids

	support_tickets_collection = db['SupportTickets']
	support_tickets = SupportTickets(user_ids)
	support_tickets_result = support_tickets.gen_data(COUNT_SUPPORTS_TICKETS)
	support_tickets_ids = support_tickets_collection.insert_many(support_tickets_result).inserted_ids

	user_recomm_collection = db['UserRecommendations']
	user_recomm = UserRecommendations(user_ids, product_ids)
	user_recomm_result = user_recomm.gen_data(COUNT_USER_RECOMMENDATIONS)
	user_recomm_ids = user_recomm_collection.insert_many(user_recomm_result).inserted_ids

	search_q_collection = db['SearchQueries']
	search_q = SearchQueries(user_ids, tuple(item['name'] for item in product_result))
	search_q_result = search_q.gen_data(COUNT_SEARCH_QUERIES)
	search_q_ids = search_q_collection.insert_many(search_q_result).inserted_ids

	event_logs_collection = db['EventLogs']
	event_logs = EventLogs(user_ids)
	event_logs_result = event_logs.gen_data(COUNT_EVENT_LOGS)
	event_logs_ids = event_logs_collection.insert_many(event_logs_result).inserted_ids

	moderation_queue_collection = db['ModerationQueue']
	moderation_queue = ModerationQueue(user_ids, product_ids)
	moderation_queue_result = moderation_queue.gen_data(COUNT_MODERATION_QUEUE)
	moderation_queue_ids = moderation_queue_collection.insert_many(moderation_queue_result).inserted_ids