CREATE TABLE company (
    id SERIAL PRIMARY KEY,
    yf_ticker VARCHAR(255),
    mw_ticker VARCHAR(255),
    yf_url VARCHAR(255),
    mw_url VARCHAR(255)
);

CREATE TABLE price_moves (
    id SERIAL PRIMARY KEY,
    news_id VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    published_date TIMESTAMP NOT NULL,
    begin_price FLOAT NOT NULL,
    end_price FLOAT NOT NULL,
    index_begin_price FLOAT NOT NULL,
    index_end_price FLOAT NOT NULL,
    volume INTEGER NOT NULL,
    market VARCHAR NOT NULL,
    price_change FLOAT NOT NULL,
    price_change_percentage FLOAT,
    index_price_change FLOAT NOT NULL,
    index_price_change_percentage FLOAT NOT NULL,
    daily_alpha FLOAT NOT NULL,
    actual_side VARCHAR(10) NOT NULL,
    predicted_side VARCHAR(10),
    predicted_move FLOAT
);