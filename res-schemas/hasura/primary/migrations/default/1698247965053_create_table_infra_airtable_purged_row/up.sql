CREATE TABLE IF NOT EXISTS infraestructure.airtable_purged_row_keys (
    id uuid PRIMARY KEY,
    key text,
    table_id text,
    purged_at timestamp without time zone
);



