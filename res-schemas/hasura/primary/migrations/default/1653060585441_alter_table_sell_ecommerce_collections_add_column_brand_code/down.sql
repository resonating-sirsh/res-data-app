-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections" add column "brand_code" text
--  null;

ALTER TABLE "sell"."ecommerce_collections"
  DROP COLUMN "brand_code";