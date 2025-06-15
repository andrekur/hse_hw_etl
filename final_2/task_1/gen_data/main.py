from models import Transactions

if __name__ == '__main__':
	transaction = Transactions()
	transaction_data = transaction.gen_array_data(1000 * 1000, True)
	transaction.export_to_txt(transaction_data)
