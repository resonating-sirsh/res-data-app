CREATE SCHEMA "sell";

CREATE TABLE "sell"."ecommerce_collections" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "title" text, "body_html" Text, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" Timestamp NOT NULL DEFAULT now(), "image_id" text, "type_collection" text NOT NULL, "store_code" text NOT NULL, "ecommerce_id" text, "published_at" timestamptz, "sort_order" Text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));COMMENT ON TABLE "sell"."ecommerce_collections" IS E'Ecommercer Collection for create.ONE';
CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_ecommerce_collections_updated_at"
BEFORE UPDATE ON "sell"."ecommerce_collections"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_ecommerce_collections_updated_at" ON "sell"."ecommerce_collections" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "sell"."ecommerce_collections" alter column "type_collection" set default 'smart';

alter table "sell"."ecommerce_collections" alter column "sort_order" set default 'manual';

CREATE TABLE "sell"."ecommerce_collection_rule" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "collection_id" uuid NOT NULL, "rules" json NOT NULL, "condition_apply" text NOT NULL DEFAULT 'any', PRIMARY KEY ("id","collection_id") , FOREIGN KEY ("collection_id") REFERENCES "sell"."ecommerce_collections"("id") ON UPDATE cascade ON DELETE cascade, UNIQUE ("id"), UNIQUE ("collection_id"));COMMENT ON TABLE "sell"."ecommerce_collection_rule" IS E'Metadata to create all the relationship between product and collection... For Smart collection, we use rules based on tag, title or others fields and custom collection are based on products_id';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "sell"."ecommerce_collection_rule" add column "created_at" timestamptz
 null default now();

alter table "sell"."ecommerce_collection_rule" add column "updated_at" timestamptz
 null default now();

CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_ecommerce_collection_rule_updated_at"
BEFORE UPDATE ON "sell"."ecommerce_collection_rule"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_ecommerce_collection_rule_updated_at" ON "sell"."ecommerce_collection_rule" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "sell"."ecommerce_collection_rule" alter column "created_at" set not null;

alter table "sell"."ecommerce_collection_rule" alter column "updated_at" set not null;

alter table "sell"."ecommerce_collection_rule" rename to "ecommerce_collections_rules";

CREATE TABLE "sell"."ecommerce_collections_sort_orders" ("id" serial NOT NULL, "name" text NOT NULL, "value" text NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") , UNIQUE ("id"), UNIQUE ("name"), UNIQUE ("value"));COMMENT ON TABLE "sell"."ecommerce_collections_sort_orders" IS E'Option to sort a collection';
CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at"
BEFORE UPDATE ON "sell"."ecommerce_collections_sort_orders"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at" ON "sell"."ecommerce_collections_sort_orders" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "sell"."ecommerce_collections_sort_orders" drop column "updated_at" cascade;

alter table "sell"."ecommerce_collections_sort_orders" drop column "created_at" cascade;

alter table "sell"."ecommerce_collections_sort_orders" add column "created_at" timestamptz
 null default now();

DROP TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at" ON "sell"."ecommerce_collections_sort_orders";

alter table "sell"."ecommerce_collections_sort_orders" add column "updated_at" timestamptz
 null default now();

CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at"
BEFORE UPDATE ON "sell"."ecommerce_collections_sort_orders"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_ecommerce_collections_sort_orders_updated_at" ON "sell"."ecommerce_collections_sort_orders" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

BEGIN TRANSACTION;
ALTER TABLE "sell"."ecommerce_collections_sort_orders" DROP CONSTRAINT "ecommerce_collections_sort_orders_pkey";

ALTER TABLE "sell"."ecommerce_collections_sort_orders"
    ADD CONSTRAINT "ecommerce_collections_sort_orders_pkey" PRIMARY KEY ("value");
COMMIT TRANSACTION;

alter table "sell"."ecommerce_collections_sort_orders" drop column "id" cascade;

alter table "sell"."ecommerce_collections_sort_orders" add column "enum_names" text
 null;

BEGIN TRANSACTION;
ALTER TABLE "sell"."ecommerce_collections_sort_orders" DROP CONSTRAINT "ecommerce_collections_sort_orders_pkey";

ALTER TABLE "sell"."ecommerce_collections_sort_orders"
    ADD CONSTRAINT "ecommerce_collections_sort_orders_pkey" PRIMARY KEY ("value", "enum_names");
COMMIT TRANSACTION;

alter table "sell"."ecommerce_collections_sort_orders" drop column "updated_at" cascade;

alter table "sell"."ecommerce_collections_sort_orders" drop column "created_at" cascade;

alter table "sell"."ecommerce_collections_sort_orders" drop column "name" cascade;

alter table "sell"."ecommerce_collections_sort_orders" rename column "value" to "description";

alter table "sell"."ecommerce_collections_sort_orders" rename column "enum_names" to "value";

alter table "sell"."ecommerce_collections_sort_orders" drop constraint "ecommerce_collections_sort_orders_value_key";
alter table "sell"."ecommerce_collections_sort_orders" add constraint "ecommerce_collections_sort_orders_value_key" unique ("value");

DROP INDEX IF EXISTS "sell"."ecommerce_collections_sort_orders_name_key";

CREATE UNIQUE INDEX "value" on
  "sell"."ecommerce_collections_sort_orders" using btree ("value");

DROP table "sell"."ecommerce_collections_sort_orders";

CREATE TABLE "sell"."ecommerce_collections_sort_order_type" ("value" text NOT NULL, "description" text, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."ecommerce_collections_sort_order_type" IS E'All the posible option to sort a collection';

INSERT INTO "sell"."ecommerce_collections_sort_order_type" VALUES
('manual', 'In the order set manually by the shop owner.'),
('alpha_asc', 'Alphabetically, in ascending order (A - Z).'),
('alpha_desc', 'Alphabetically, in descending order (Z - A).'),
('created', 'By date created, in ascending order (oldest - newest).est-selling products.'),
('best_selling', 'By best-selling products.'),
('created_desc', 'By date created, in descending order (newest - oldest).v'),
('price_asc', 'By price, in ascending order (lowest - highest).'),
('price_desc', 'By price, in descending order (highest - lowest).')
;

alter table "sell"."ecommerce_collections" add column "sort_order_fk" text
 null;

alter table "sell"."ecommerce_collections"
  add constraint "ecommerce_collections_sort_order_fk_fkey"
  foreign key ("sort_order_fk")
  references "sell"."ecommerce_collections_sort_order_type"
  ("value") on update no action on delete no action;

alter table "sell"."ecommerce_collections" drop column "sort_order" cascade;

alter table "sell"."ecommerce_collections" add column "handle" text
 null;

alter table "sell"."ecommerce_collections" alter column "handle" set not null;
