CREATE TABLE company (
    id SERIAL PRIMARY KEY,
    yf_ticker VARCHAR(255),
    mw_ticker VARCHAR(255),
    yf_url VARCHAR(255),
    mw_url VARCHAR(255)
);