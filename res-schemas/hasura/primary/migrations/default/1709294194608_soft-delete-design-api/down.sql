
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."designs" add column "deleted_at" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."colors_artworks" add column "deleted_at" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."colors_artworks" add column "updated_at" timestamptz
--  null default now();
--
-- CREATE OR REPLACE FUNCTION "meta"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_meta_colors_artworks_updated_at"
-- BEFORE UPDATE ON "meta"."colors_artworks"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_meta_colors_artworks_updated_at" ON "meta"."colors_artworks"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."colors" add column "deleted_at" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."artworks" add column "deleted_at" timestamptz
--  null;
