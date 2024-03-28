-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_pieces" add column "deleted_at" timestamptz
--  null;
alter table "meta"."body_pieces" drop column "deleted_at"