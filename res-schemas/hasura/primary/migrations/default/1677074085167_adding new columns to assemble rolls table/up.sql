
alter table "make"."assemble_rolls" add column "roll_primary_key" text
 not null unique;

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "standard_cost_per_unit" TYPE Float;

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "standard_cost_per_unit" TYPE float4;

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "roll_length" TYPE float4;

alter table "make"."assemble_rolls" drop column "id" cascade;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
alter table "make"."assemble_rolls" add column "id" uuid
 not null unique default gen_random_uuid();

ALTER TABLE "make"."assemble_rolls" ALTER COLUMN "id" drop default;
alter table "make"."assemble_rolls" alter column "id" drop not null;

alter table "make"."assemble_rolls" alter column "id" set default gen_random_uuid();
