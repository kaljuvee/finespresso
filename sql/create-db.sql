CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    link VARCHAR(255),
    description TEXT,
    published_date TIMESTAMP WITH TIME ZONE,
    summary TEXT,
    ai_insights TEXT
);