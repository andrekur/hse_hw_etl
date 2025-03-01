from models import (
  User, UserSession, Product, ProductPriceHistory, SupportTickets, UserRecommendations, SearchQueries,
  EventLogs, ModerationQueue
)

from pymongo import MongoClient

username = "root"
password = "example"
host = "mongo_app"
port = 27017

# TODO .env
uri = f"mongodb://{username}:{password}@{host}:{port}/"


if __name__ == '__main__':
	client = MongoClient(uri) # TODO env

	db = client['shop']

	user_collection = db['Users']
	user = User()
	user_result = user.gen_data(1000)
	user_ids = user_collection.insert_many(user_result).inserted_ids

	u_s_collection = db['UserSessions']
	user_session = UserSession(user_ids)
	session_result = user_session.gen_data(2000)
	session_ids = u_s_collection.insert_many(session_result)

	product_collection = db['Products']
	product = Product()
	product_result = product.gen_data(200)
	product_ids = product_collection.insert_many(product_result).inserted_ids

	product_p_h_collection = db['ProductPriceHistory']
	product_p_h = ProductPriceHistory(product_ids.copy())
	product_p_h_result = product_p_h.gen_data(200)
	product_p_h_ids = product_p_h_collection.insert_many(product_p_h_result).inserted_ids

	support_tickets_collection = db['SupportTickets']
	support_tickets = SupportTickets(user_ids)
	support_tickets_result = support_tickets.gen_data(3000)
	support_tickets_ids = support_tickets_collection.insert_many(support_tickets_result).inserted_ids

	user_recomm_collection = db['UserRecommendations']
	user_recomm = UserRecommendations(user_ids, product_ids)
	user_recomm_result = user_recomm.gen_data(2000)
	user_recomm_ids = user_recomm_collection.insert_many(user_recomm_result).inserted_ids

	search_q_collection = db['SearchQueries']
	search_q = SearchQueries(user_ids, tuple(item['name'] for item in product_result))
	search_q_result = search_q.gen_data(2000)
	search_q_ids = search_q_collection.insert_many(search_q_result).inserted_ids

	event_logs_collection = db['EventLogs']
	event_logs = EventLogs(user_ids)
	event_logs_result = event_logs.gen_data(2000)
	event_logs_ids = event_logs_collection.insert_many(event_logs_result).inserted_ids

	moderation_queue_collection = db['ModerationQueue']
	moderation_queue = ModerationQueue(user_ids, product_ids)
	moderation_queue_result = moderation_queue.gen_data(2000)
	moderation_queue_ids = moderation_queue_collection.insert_many(moderation_queue_result).inserted_ids