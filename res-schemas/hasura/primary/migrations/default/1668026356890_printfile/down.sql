
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."printfile" add column "metadata" jsonb
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."printfile_pieces" add column "metadata" jsonb
--  null;

DROP TABLE "make"."printfile_pieces" CASCADE;

DROP TABLE "make"."printfile" CASCADE;
