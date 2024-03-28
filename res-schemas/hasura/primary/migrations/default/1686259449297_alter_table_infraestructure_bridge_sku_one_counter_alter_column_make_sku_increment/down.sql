alter table "infraestructure"."bridge_sku_one_counter" rename column "airtable_rid" to "make_sku_increment";
ALTER TABLE "infraestructure"."bridge_sku_one_counter" ALTER COLUMN "make_sku_increment" TYPE integer;
