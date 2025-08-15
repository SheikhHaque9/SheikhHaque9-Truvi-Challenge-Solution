-- final_table.sql
-- Creates the "final_table" summarizing monthly revenue per company in both original currency and GBP.
-- Combines bookings, country_currency, and currency_rates to calculate total revenue per month.
-- Steps:
-- 1. monthly_rates: Calculates average monthly exchange rates to GBP.
-- 2. monthly_revenue: Calculates monthly booking fees per company and ensures a minimum fee per country.
-- 3. final_revenue: Ensures total revenue is at least the minimum fee.
-- 4. Final SELECT: Converts revenue to GBP using average monthly rates.

CREATE TABLE IF NOT EXISTS final_table AS
WITH monthly_rates AS (
    SELECT 
        from_currency,
        to_currency,
        DATE_TRUNC('month', rate_date) AS month_start,
        AVG(rate) AS avg_rate
    FROM currency_rates
    WHERE to_currency = 'GBP'
    GROUP BY from_currency, to_currency, DATE_TRUNC('month', rate_date)
),
monthly_revenue AS (
    SELECT
        b.owner_company,
        b.owner_company_country,
        cc.currency AS revenue_currency,
        DATE_TRUNC('month', b.check_out_date) AS month_start,
        SUM(
            CASE
                WHEN LOWER(b.owner_company_country) = 'uk'  THEN 10
                WHEN LOWER(b.owner_company_country) = 'usa' THEN 14
                ELSE 12
            END
        ) AS total_booking_fee,
        CASE
            WHEN LOWER(b.owner_company_country) = 'uk'  THEN 100
            WHEN LOWER(b.owner_company_country) = 'usa' THEN 140
            ELSE 120
        END AS minimum_fee
    FROM bookings b
    LEFT JOIN country_currency cc 
        ON LOWER(b.owner_company_country) = LOWER(cc.country)
    GROUP BY b.owner_company, b.owner_company_country, cc.currency, DATE_TRUNC('month', b.check_out_date)
),
final_revenue AS (
    SELECT
        owner_company,
        owner_company_country,
        revenue_currency,
        month_start,
        GREATEST(total_booking_fee, minimum_fee) AS total_revenue_original
    FROM monthly_revenue
)
SELECT
    fr.owner_company,
    TO_CHAR(fr.month_start, 'MM') AS month,
    fr.total_revenue_original AS original_monthly_revenue,
    COALESCE(fr.total_revenue_original * mr.avg_rate, fr.total_revenue_original) AS monthly_revenue_in_gbp
FROM final_revenue fr
LEFT JOIN monthly_rates mr
    ON fr.revenue_currency = mr.from_currency
    AND fr.month_start = mr.month_start;
