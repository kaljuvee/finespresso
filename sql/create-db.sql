CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    link VARCHAR(255),
    company VARCHAR(255),
    published_date TIMESTAMP WITH TIME ZONE,
    content TEXT,
    ai_summary TEXT,
    industry VARCHAR(255),
    publisher_topic VARCHAR(255),
    ai_topic VARCHAR(255),
    publisher VARCHAR(255)
);