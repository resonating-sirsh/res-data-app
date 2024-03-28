SELECT 'something is broken';

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "meta"."bodies" add column "body_piece_count_checksum" integer
-- --  null;


-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "finance"."material_rates" add column "name" text
-- --  null;

-- ALTER TABLE "meta"."style_sizes" ALTER COLUMN "piece_count_checksum" TYPE numeric;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "meta"."style_sizes" add column "piece_count_checksum" numeric
-- --  null;

-- alter table "meta"."style_sizes" rename column "material_usage_statistics" to "material_usage";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "meta"."style_sizes" add column "material_usage" jsonb
-- --  null default jsonb_build_array();

-- alter table "meta"."bodies" rename column "estimated_sewing_time" to "estimated_sewing_costs";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "finance"."material_rates" add column "ended_at" timestamptz
-- --  null;

-- comment on column "finance"."material_rates"."ended_at" is E'material rate tracking history';
-- alter table "finance"."material_rates" alter column "ended_at" drop not null;
-- alter table "finance"."material_rates" add column "ended_at" numeric;

-- alter table "meta"."bodies" rename column "estimated_sewing_costs" to "sewing_estimated_costs";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "finance"."material_rates" add column "material_code" text
-- --  not null;

-- DROP TABLE "finance"."material_rates";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "finance"."node_rates" add column "ended_at" timestamptz
-- --  null;

-- DROP TABLE "finance"."node_rates";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- DROP table "flow"."node_rates";

-- DROP TABLE "flow"."node_rates";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- DROP table "flow"."finance_node_rates";

-- drop schema "finance" cascade;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "meta"."style_sizes" add column "size_yield" numeric
-- --  null;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "meta"."bodies" add column "sewing_estimated_costs" numeric
-- --  null;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "meta"."bodies" add column "trim_costs" numeric
-- --  null;

-- DROP TABLE "flow"."finance_node_rates";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "dxa"."flow_node_run" add column "auto_retries" integer
-- --  not null default '0';
-- alter table "dxa"."flow_node_run" drop column if exists "auto_retries"

-- alter table "sell"."transactions" drop column "is_failed_transaction" cascade;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "infraestructure"."bridge_sku_one_counter" add column "one_number" integer
-- --  null;

-- alter table "infraestructure"."bridge_sku_one_counter" alter column "one_number" drop not null;
-- alter table "infraestructure"."bridge_sku_one_counter" add column "one_number" text;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "infraestructure"."bridge_sku_one_counter" add column "order_number" text
-- --  null;

-- alter table "infraestructure"."bridge_sku_one_counter" rename column "airtable_rid" to "make_sku_increment";
-- ALTER TABLE "infraestructure"."bridge_sku_one_counter" ALTER COLUMN "make_sku_increment" TYPE integer;

-- DROP TABLE "infraestructure"."bridge_sku_one_counter";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."one_piece_healing_requests" add column "piece_id" uuid
-- --  null;

-- DROP TABLE "make"."one_piece_healing_requests";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."make_order_in_inventory" add column "checkin_type" Text
-- --  null;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."make_order_in_inventory" add column "work_station" text
-- --  null;

-- DROP TABLE "make"."make_inventory_checkout";

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."make_order_in_inventory" add column "one_order_id" uuid
-- --  null;

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."make_order_in_inventory" add column "metadata" jsonb
-- --  null default jsonb_build_object();

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."make_order_in_inventory" add column "updated_at" timestamptz
-- --  null default now();
-- --
-- -- CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
-- -- RETURNS TRIGGER AS $$
-- -- DECLARE
-- --   _new record;
-- -- BEGIN
-- --   _new := NEW;
-- --   _new."updated_at" = NOW();
-- --   RETURN _new;
-- -- END;
-- -- $$ LANGUAGE plpgsql;
-- -- CREATE TRIGGER "set_make_make_order_in_inventory_updated_at"
-- -- BEFORE UPDATE ON "make"."make_order_in_inventory"
-- -- FOR EACH ROW
-- -- EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
-- -- COMMENT ON TRIGGER "set_make_make_order_in_inventory_updated_at" ON "make"."make_order_in_inventory"
-- -- IS 'trigger to set value of column "updated_at" to current timestamp on row update';

-- -- Could not auto-generate a down migration.
-- -- Please write an appropriate down migration for the SQL below:
-- -- alter table "make"."make_order_in_inventory" add column "created_at" timestamptz
-- --  null default now();

-- alter table "make"."make_order_in_inventory" rename column "id" to "record_id";

-- alter table "make"."make_order_in_inventory" alter column "order_name" drop not null;
-- alter table "make"."make_order_in_inventory" add column "order_name" text;

-- alter table "make"."make_order_in_inventory" rename to "inventory";

-- alter table "make"."inventory" rename column "sku_bar_code_scanned" to "bar_code_scanned";

-- alter table "make"."inventory" alter column "sku" drop not null;
-- alter table "make"."inventory" add column "sku" text;

-- alter table "make"."inventory" alter column "group_fulfillment_id" drop not null;
-- alter table "make"."inventory" add column "group_fulfillment_id" text;

-- alter table "make"."inventory" alter column "order_line_item_id" drop not null;
-- alter table "make"."inventory" add column "order_line_item_id" text;

-- alter table "make"."inventory" rename column "group_fulfillment_id" to "order_id";

-- DROP TABLE "make"."inventory";
