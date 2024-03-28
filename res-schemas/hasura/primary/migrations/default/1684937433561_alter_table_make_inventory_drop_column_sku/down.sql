alter table "make"."inventory" alter column "sku" drop not null;
alter table "make"."inventory" add column "sku" text;
