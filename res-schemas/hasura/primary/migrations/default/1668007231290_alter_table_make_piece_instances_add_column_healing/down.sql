-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."piece_instances" add column "healing" boolean
--  null default 'false';
alter table "make"."piece_instances" alter column "healing" drop not null;
