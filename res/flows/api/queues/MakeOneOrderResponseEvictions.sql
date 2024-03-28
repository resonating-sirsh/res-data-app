
WITH MaxProcessed AS (
    SELECT key, MAX(processed_at) AS max_processed_at
    FROM infraestructure.airtable_updates 
    WHERE metadata->>'table_name' = 'One Order Responses'
    GROUP BY key
)

SELECT m.key --, m.max_processed_at, a.updated_at
FROM MaxProcessed m
JOIN infraestructure.airtable_updates a
ON m.key = a.key AND m.max_processed_at = a.processed_at
WHERE metadata->>'table_name' = 'One Order Responses'
and m.max_processed_at >= %s
and m.max_processed_at <= %s 
ORDER BY m.max_processed_at ASC;

