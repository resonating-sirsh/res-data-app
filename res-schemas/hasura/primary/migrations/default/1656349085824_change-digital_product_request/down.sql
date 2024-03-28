
alter table "digital_product"."digital_product_request" alter column "contract_address" set default '0'::text;

ALTER TABLE "digital_product"."digital_product_request" ALTER COLUMN "to_address" drop default;

ALTER TABLE "digital_product"."digital_product_request" ALTER COLUMN "token_id" drop default;

ALTER TABLE "digital_product"."digital_product_request" ALTER COLUMN "contract_address" drop default;

alter table "digital_product"."digital_product_request" alter column "token_id" drop not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "to_address" text
--  not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "token_id" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "contract_address" text
--  not null;
