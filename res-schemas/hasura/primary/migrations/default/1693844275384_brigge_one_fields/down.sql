
alter table "infraestructure"."bridge_sku_one_counter" rename column "print_assets_cached_at" to "print_assets_annotated_at";

alter table "infraestructure"."bridge_sku_one_counter" rename column "dxa_assets_ready_at" to "dxa_assets_verified_at";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "dxa_assets_verified_at" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "sales_channel" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "print_assets_annotated_at" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "print_request_airtable_id" text
--  null;
