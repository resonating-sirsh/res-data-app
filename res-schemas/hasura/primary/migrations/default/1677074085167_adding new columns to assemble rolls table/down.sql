
ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "id" drop default;

alter table "make"."assemble_rolls" alter column "id" set not null;
alter table "make"."assemble_rolls" alter column "id" set default gen_random_uuid();

alter table "make"."assemble_rolls" drop column "id" cascade
alter table "make"."assemble_rolls" drop column "id";
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

comment on column "make"."assemble_rolls"."id" is E'In this table live the rolls data';
alter table "make"."assemble_rolls" alter column "id" set default gen_random_uuid();
alter table "make"."assemble_rolls" add constraint "assemble_rolls_id_key" unique (id);
alter table "make"."assemble_rolls" alter column "id" drop not null;
alter table "make"."assemble_rolls" add column "id" uuid;

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "roll_length" TYPE numeric;

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "standard_cost_per_unit" TYPE double precision;

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "standard_cost_per_unit" TYPE numeric;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."assemble_rolls" add column "roll_primary_key" text
--  not null unique;
