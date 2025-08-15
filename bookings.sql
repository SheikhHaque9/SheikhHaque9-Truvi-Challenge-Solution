-- bookings.sql
-- Creates the "bookings" table if it does not already exist.
-- Stores booking records with unique IDs, check-in/out dates, and owner company information.

CREATE TABLE IF NOT EXISTS bookings (
    booking_id UUID PRIMARY KEY,
    check_in_date DATE,
    check_out_date DATE,
    owner_company VARCHAR(255),
    owner_company_country VARCHAR(255)
);
