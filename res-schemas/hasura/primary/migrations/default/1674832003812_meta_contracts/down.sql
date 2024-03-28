
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "updated_at" timestamptz
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
-- CREATE TRIGGER "set_meta_pieces_updated_at"
-- BEFORE UPDATE ON "meta"."pieces"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_meta_pieces_updated_at" ON "meta"."pieces"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "created_at" timestamptz
--  null default now();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."styles" add column "contracts_failing" jsonb
--  null default jsonb_build_array();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."bodies" add column "contracts_failing" jsonb
--  null default jsonb_build_array();
