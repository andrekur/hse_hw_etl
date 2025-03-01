CREATE TABLE IF NOT EXISTS "Users" (
    user_id UUID PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(60),
    phone VARCHAR(14),
    registration_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS "UserSessions" (
    session_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device TEXT,
    actions TEXT[]
);

CREATE TABLE IF NOT EXISTS "Products" (
    product_id UUID PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT NOT NULL,
    category_id INT NOT NULL,
    creation_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS "ProductPriceHistory" (
    product_id UUID PRIMARY KEY,
    price_changes JSONB,
    current_price NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS "SupportTickets" (
    ticket_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    messages TEXT[],
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    end_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "UserRecommendations" (
    recom_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    recommended_products UUID[],
    last_updated TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS "SearchQueries" (
    query_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    query_text TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    filters VARCHAR(100),
    results_count INTEGER
);

CREATE TABLE IF NOT EXISTS "EventLogs" (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details JSONB
);

CREATE TABLE IF NOT EXISTS "ModerationQueue" (
    review_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    review_text TEXT NOT NULL,
    rating SMALLINT,
    moderation_status VARCHAR(50) NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    review_end TIMESTAMP
);

