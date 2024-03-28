SELECT 'something is broken';

-- CREATE TABLE "make"."inventory" ("record_id" uuid NOT NULL DEFAULT gen_random_uuid(), "one_number" text NOT NULL, "reservation_status" text NOT NULL, "bin_location" text NOT NULL, "order_line_item_id" text NOT NULL, "order_id" text NOT NULL, "order_name" text NOT NULL, "bar_code_scanned" text NOT NULL, "sku" text NOT NULL, "warehouse_checkin_location" text NOT NULL, "operator_name" text NOT NULL, PRIMARY KEY ("record_id") , UNIQUE ("record_id"));
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- alter table "make"."inventory" rename column "order_id" to "group_fulfillment_id";

-- alter table "make"."inventory" drop column "order_line_item_id" cascade;

-- alter table "make"."inventory" drop column "group_fulfillment_id" cascade;

-- alter table "make"."inventory" drop column "sku" cascade;

-- alter table "make"."inventory" rename column "bar_code_scanned" to "sku_bar_code_scanned";

-- alter table "make"."inventory" rename to "make_order_in_inventory";

-- alter table "make"."make_order_in_inventory" drop column "order_name" cascade;

-- alter table "make"."make_order_in_inventory" rename column "record_id" to "id";

-- alter table "make"."make_order_in_inventory" add column "created_at" timestamptz
--  null default now();

-- alter table "make"."make_order_in_inventory" add column "updated_at" timestamptz
--  null default now();

-- CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_make_make_order_in_inventory_updated_at"
-- BEFORE UPDATE ON "make"."make_order_in_inventory"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_make_make_order_in_inventory_updated_at" ON "make"."make_order_in_inventory"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';

-- alter table "make"."make_order_in_inventory" add column "metadata" jsonb
--  null default jsonb_build_object();

-- alter table "make"."make_order_in_inventory" add column "one_order_id" uuid
--  null;

-- CREATE TABLE "make"."make_inventory_checkout" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "metadata" jsonb DEFAULT jsonb_build_object(), "checkout_at" timestamptz NOT NULL, "make_inventory_id" uuid NOT NULL, "fulfillment_item_id" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("make_inventory_id") REFERENCES "make"."make_order_in_inventory"("id") ON UPDATE restrict ON DELETE restrict);
-- CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_make_make_inventory_checkout_updated_at"
-- BEFORE UPDATE ON "make"."make_inventory_checkout"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_make_make_inventory_checkout_updated_at" ON "make"."make_inventory_checkout"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- alter table "make"."make_order_in_inventory" add column "work_station" text
--  null;

-- alter table "make"."make_order_in_inventory" add column "checkin_type" Text
--  null;

-- CREATE TABLE "make"."one_piece_healing_requests" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "request_id" text NOT NULL, "metadata" jsonb NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "make"."one_piece_healing_requests" IS E'records every unqiue request by unique request if to heal a piece';
-- CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_make_one_piece_healing_requests_updated_at"
-- BEFORE UPDATE ON "make"."one_piece_healing_requests"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_make_one_piece_healing_requests_updated_at" ON "make"."one_piece_healing_requests"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- alter table "make"."one_piece_healing_requests" add column "piece_id" uuid
--  null;

-- CREATE TABLE "infraestructure"."bridge_sku_one_counter" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "cancelled_at" timestamptz, "one_number" text NOT NULL, "sku" text NOT NULL, "style_sku" text NOT NULL, "make_sku_increment" integer NOT NULL, PRIMARY KEY ("id") );
-- CREATE OR REPLACE FUNCTION "infraestructure"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_infraestructure_bridge_sku_one_counter_updated_at"
-- BEFORE UPDATE ON "infraestructure"."bridge_sku_one_counter"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_infraestructure_bridge_sku_one_counter_updated_at" ON "infraestructure"."bridge_sku_one_counter"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ALTER TABLE "infraestructure"."bridge_sku_one_counter" ALTER COLUMN "make_sku_increment" TYPE text;
-- alter table "infraestructure"."bridge_sku_one_counter" rename column "make_sku_increment" to "airtable_rid";

-- alter table "infraestructure"."bridge_sku_one_counter" add column "order_number" text
--  null;

-- alter table "infraestructure"."bridge_sku_one_counter" drop column "one_number" cascade;

-- alter table "infraestructure"."bridge_sku_one_counter" add column "one_number" integer
--  null;

-- alter table "sell"."transactions" add column "is_failed_transaction" boolean
--  not null default 'True';

-- alter table "dxa"."flow_node_run" add column if not exists "auto_retries" integer
--  not null default '0';

-- CREATE TABLE "flow"."finance_node_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "ended_at" timestamptz, "sew_rate_minutes" numeric NOT NULL, "print_rate_yards" numeric NOT NULL, "cut_rate_units" numeric NOT NULL, "default_overhead" numeric NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "flow"."finance_node_rates" IS E'a placeholder for storing rates snapshots until some more sophisticated comes along';
-- CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_flow_finance_node_rates_updated_at"
-- BEFORE UPDATE ON "flow"."finance_node_rates"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_flow_finance_node_rates_updated_at" ON "flow"."finance_node_rates"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- alter table "meta"."bodies" add column "trim_costs" numeric
--  null;

-- alter table "meta"."bodies" add column "sewing_estimated_costs" numeric
--  null;

-- alter table "meta"."style_sizes" add column "size_yield" numeric
--  null;

-- create schema "finance";

-- DROP table "flow"."finance_node_rates";

-- CREATE TABLE "flow"."node_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "ended_at" timestamptz, "sew_node_rate_minutes" numeric NOT NULL DEFAULT 019799, "print_node_rate_yards" numeric NOT NULL DEFAULT 1.83, "cut_node_rate_pieces" numeric NOT NULL DEFAULT 0.15, "default_overhead" numeric NOT NULL DEFAULT 0.2, PRIMARY KEY ("id") );COMMENT ON TABLE "flow"."node_rates" IS E'history table with all flat rates - track changes but change slowly. should re-model';
-- CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_flow_node_rates_updated_at"
-- BEFORE UPDATE ON "flow"."node_rates"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_flow_node_rates_updated_at" ON "flow"."node_rates"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- DROP table "flow"."node_rates";

-- CREATE TABLE "finance"."node_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "sew_node_rate_minutes" numeric NOT NULL DEFAULT .019799, "print_node_rate_yards" numeric NOT NULL DEFAULT 1.83, "cut_node_rate_pieces" numeric NOT NULL DEFAULT .15, "default_overhead" numeric NOT NULL DEFAULT .2, PRIMARY KEY ("id") );COMMENT ON TABLE "finance"."node_rates" IS E'history table showing changes of rates summary';
-- CREATE OR REPLACE FUNCTION "finance"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_finance_node_rates_updated_at"
-- BEFORE UPDATE ON "finance"."node_rates"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "finance"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_finance_node_rates_updated_at" ON "finance"."node_rates"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- alter table "finance"."node_rates" add column "ended_at" timestamptz
--  null;

-- CREATE TABLE "finance"."material_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "fabric_price" numeric NOT NULL, "ended_at" numeric NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "finance"."material_rates" IS E'material rate tracking history';
-- CREATE OR REPLACE FUNCTION "finance"."set_current_timestamp_updated_at"()
-- RETURNS TRIGGER AS $$
-- DECLARE
--   _new record;
-- BEGIN
--   _new := NEW;
--   _new."updated_at" = NOW();
--   RETURN _new;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER "set_finance_material_rates_updated_at"
-- BEFORE UPDATE ON "finance"."material_rates"
-- FOR EACH ROW
-- EXECUTE PROCEDURE "finance"."set_current_timestamp_updated_at"();
-- COMMENT ON TRIGGER "set_finance_material_rates_updated_at" ON "finance"."material_rates"
-- IS 'trigger to set value of column "updated_at" to current timestamp on row update';
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- alter table "finance"."material_rates" add column "material_code" text
--  not null;

-- alter table "meta"."bodies" rename column "sewing_estimated_costs" to "estimated_sewing_costs";

-- alter table "finance"."material_rates" drop column "ended_at" cascade;

-- alter table "finance"."material_rates" add column "ended_at" timestamptz
--  null;

-- alter table "meta"."bodies" rename column "estimated_sewing_costs" to "estimated_sewing_time";

-- alter table "meta"."style_sizes" add column "material_usage" jsonb
--  null default jsonb_build_array();

-- alter table "meta"."style_sizes" rename column "material_usage" to "material_usage_statistics";

-- alter table "meta"."style_sizes" add column "piece_count_checksum" numeric
--  null;

--   ALTER COLUMN "piece_count_checksum" TYPE int4;

-- alter table "finance"."material_rates" add column "name" text
--  null;

-- alter table "meta"."bodies" add column "body_piece_count_checksum" integer
--  null;
