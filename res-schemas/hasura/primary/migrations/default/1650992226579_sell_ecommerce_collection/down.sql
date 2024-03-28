
alter table "sell"."ecommerce_collections" drop constraint "ecommerce_collections_status_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections" add column "status" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections" add column "disjuntive" boolean
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections" add column "rules" json
--  null;

comment on column "sell"."timestamptz" is E'Ecommercer Collection for create.ONE';
alter table "sell"."ecommerce_collections" alter column "imported_at" drop not null;
alter table "sell"."ecommerce_collections" add column "imported_at" timestamptz;

DROP TABLE "sell"."ecommerce_collection_status";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "sell"."ecommerce_collections_rules";
