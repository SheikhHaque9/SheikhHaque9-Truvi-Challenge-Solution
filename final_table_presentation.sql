-- final_table_presentation.sql
-- Simple query to retrieve final_table.
-- Orders by owner company and month for readability.

SELECT *
FROM final_table
ORDER BY owner_company, month;
