CREATE TABLE transactions (
    transaction_id Int64,
    user_id Int64,
    amount Int64,
    currency String,
    transaction_date Timestamp,
    is_fraud Bool,
    PRIMARY KEY (transaction_id)
);
