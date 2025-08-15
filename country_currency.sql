-- country_currency.sql
-- Creates the "country_currency" table if it does not exist.
-- Maps countries to their respective currencies.
-- Inserts example data for UK, USA, and France.

CREATE TABLE IF NOT EXISTS country_currency (
    country TEXT PRIMARY KEY,
    currency TEXT NOT NULL
);

INSERT INTO country_currency (country, currency) VALUES
('UK', 'GBP'),
('USA', 'USD'),
('FRANCE', 'EUR');
