
alter table "sell"."ecommerce_collections" alter column "handle" drop not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections" add column "handle" text
--  null;


comment on column "sell"."text" is E'Ecommercer Collection for create.ONE';
alter table "sell"."ecommerce_collections" alter column "sort_order" set default ''manual'::text';
alter table "sell"."ecommerce_collections" alter column "sort_order" drop not null;
alter table "sell"."ecommerce_collections" add column "sort_order" text;

alter table "sell"."ecommerce_collections" drop constraint "ecommerce_collections_sort_order_fk_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections" add column "sort_order_fk" text
--  null;

DROP TABLE "sell"."ecommerce_collections_sort_order_type";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "sell"."ecommerce_collections_sort_orders";

DROP INDEX IF EXISTS "sell"."value";

CREATE  INDEX "ecommerce_collections_sort_orders_name_key" on
  "sell"."ecommerce_collections_sort_orders" using btree ("name");

alter table "sell"."ecommerce_collections_sort_orders" drop constraint "ecommerce_collections_sort_orders_value_key";
alter table "sell"."ecommerce_collections_sort_orders" add constraint "ecommerce_collections_sort_orders_description_key" unique ("description");

alter table "sell"."ecommerce_collections_sort_orders" rename column "value" to "enum_names";

alter table "sell"."ecommerce_collections_sort_orders" rename column "description" to "value";

comment on column "sell"."text" is E'Option to sort a collection';
alter table "sell"."ecommerce_collections_sort_orders" add constraint "ecommerce_collections_sort_orders_name_key" unique (name);
alter table "sell"."ecommerce_collections_sort_orders" alter column "name" drop not null;
alter table "sell"."ecommerce_collections_sort_orders" add column "name" text;

comment on column "sell"."timestamptz" is E'Option to sort a collection';
alter table "sell"."ecommerce_collections_sort_orders" alter column "created_at" set default now();
alter table "sell"."ecommerce_collections_sort_orders" alter column "created_at" drop not null;
alter table "sell"."ecommerce_collections_sort_orders" add column "created_at" timestamptz;

comment on column "sell"."timestamptz" is E'Option to sort a collection';
alter table "sell"."ecommerce_collections_sort_orders" alter column "updated_at" set default now();
alter table "sell"."ecommerce_collections_sort_orders" alter column "updated_at" drop not null;
alter table "sell"."ecommerce_collections_sort_orders" add column "updated_at" timestamptz;

alter table "sell"."ecommerce_collections_sort_orders" drop constraint "ecommerce_collections_sort_orders_pkey";
alter table "sell"."ecommerce_collections_sort_orders"
    add constraint "ecommerce_collections_sort_orders_pkey"
    primary key ("value");

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections_sort_orders" add column "enum_names" text
--  null;

comment on column "sell"."int4" is E'Option to sort a collection';
alter table "sell"."ecommerce_collections_sort_orders" alter column "id" set default nextval('sell.ecommerce_collections_sort_orders_id_seq'::regclass);
alter table "sell"."ecommerce_collections_sort_orders" alter column "id" drop not null;
alter table "sell"."ecommerce_collections_sort_orders" add column "id" int4;

alter table "sell"."ecommerce_collections_sort_orders" drop constraint "ecommerce_collections_sort_orders_pkey";
alter table "sell"."ecommerce_collections_sort_orders"
    add constraint "ecommerce_collections_sort_orders_pkey"
    primary key ("id");

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections_sort_orders" add column "updated_at" timestamptz
--  null default now();
--
-- CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at"
-- BEFORE UPDATE ON "sell"."ecommerce_collections_sort_orders"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at" ON "sell"."ecommerce_collections_sort_orders"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';

CREATE TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at"
BEFORE UPDATE ON "sell"."ecommerce_collections_sort_orders"
FOR EACH ROW EXECUTE FUNCTION sell.set_current_timestamp_updated_at();COMMENT ON TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at" ON "sell"."ecommerce_collections_sort_orders"
IS E'trigger to set value of column "updated_at" to current timestamp on row update';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collections_sort_orders" add column "created_at" timestamptz
--  null default now();

comment on column "sell"."timestamptz" is E'Option to sort a collection';
alter table "sell"."ecommerce_collections_sort_orders" alter column "created_at" set default now();
alter table "sell"."ecommerce_collections_sort_orders" alter column "created_at" drop not null;
alter table "sell"."ecommerce_collections_sort_orders" add column "created_at" timestamptz;

comment on column "sell"."timestamptz" is E'Option to sort a collection';
alter table "sell"."ecommerce_collections_sort_orders" alter column "updated_at" set default now();
alter table "sell"."ecommerce_collections_sort_orders" alter column "updated_at" drop not null;
alter table "sell"."ecommerce_collections_sort_orders" add column "updated_at" timestamptz;

DROP TABLE "sell"."ecommerce_collections_sort_orders";

alter table "sell"."ecommerce_collections_rules" rename to "ecommerce_collection_rule";

alter table "sell"."ecommerce_collection_rule" alter column "updated_at" drop not null;

alter table "sell"."ecommerce_collection_rule" alter column "created_at" drop not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collection_rule" add column "updated_at" timestamptz
--  null default now();
--
-- CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_sell_ecommerce_collection_rule_updated_at"
-- BEFORE UPDATE ON "sell"."ecommerce_collection_rule"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_sell_ecommerce_collection_rule_updated_at" ON "sell"."ecommerce_collection_rule"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."ecommerce_collection_rule" add column "created_at" timestamptz
--  null default now();

DROP TABLE "sell"."ecommerce_collection_rule";

ALTER TABLE "sell"."ecommerce_collections" ALTER COLUMN "sort_order" drop default;

ALTER TABLE "sell"."ecommerce_collections" ALTER COLUMN "type_collection" drop default;

DROP TABLE "sell"."ecommerce_collections";
