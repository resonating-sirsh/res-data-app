ALTER TABLE "infraestructure"."bridge_sku_one_counter" ALTER COLUMN "make_sku_increment" TYPE text;
alter table "infraestructure"."bridge_sku_one_counter" rename column "make_sku_increment" to "airtable_rid";
