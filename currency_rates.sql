-- currency_rates.sql
-- Creates the "currency_rates" table if it does not exist.
-- Stores exchange rates between two currencies, including the date of the rate.
-- Uses a SERIAL primary key for unique identification of each rate entry.

CREATE TABLE IF NOT EXISTS currency_rates (
    exchange_id SERIAL PRIMARY KEY,
    from_currency VARCHAR(255),
    to_currency VARCHAR(255),
    rate DECIMAL,
    rate_date DATE
);