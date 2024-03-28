alter table "infraestructure"."bridge_sku_one_counter" alter column "one_number" drop not null;
alter table "infraestructure"."bridge_sku_one_counter" add column "one_number" text;
