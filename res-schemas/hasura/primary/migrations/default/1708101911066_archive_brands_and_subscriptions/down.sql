-- Down migration

-- Drop triggers
DROP TRIGGER IF EXISTS trg_archive_brands ON sell.brands;
DROP TRIGGER IF EXISTS trg_archive_subscriptions ON sell.subscriptions;

-- Drop trigger functions
DROP FUNCTION IF EXISTS fn_archive_brands();
DROP FUNCTION IF EXISTS fn_archive_subscriptions();

-- Remove new columns
ALTER TABLE sell.brands DROP COLUMN IF EXISTS modified_by;
ALTER TABLE sell.brands DROP COLUMN IF EXISTS created_at;
ALTER TABLE sell.brands DROP COLUMN IF EXISTS updated_at;
ALTER TABLE sell.subscriptions DROP COLUMN IF EXISTS modified_by;

-- Drop archive tables
DROP TABLE IF EXISTS sell.brands_archive;
DROP TABLE IF EXISTS sell.subscriptions_archive;
