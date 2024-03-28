
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."machine_configs" add column "metadata" jsonb
--  not null default jsonb_build_object();

alter table "make"."machine_configs" alter column "serial_number" drop not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."machine_configs" add column "serial_number" Text
--  null unique;

DROP TABLE "make"."machine_configs";
