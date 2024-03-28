
alter table "meta"."bodies" add column "trim_costs" numeric
 null;

alter table "meta"."bodies" add column "sewing_estimated_costs" numeric
 null;

alter table "meta"."style_sizes" add column "size_yield" numeric
 null;

create schema "finance";

DROP table "flow"."finance_node_rates";

CREATE TABLE "flow"."node_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "ended_at" timestamptz, "sew_node_rate_minutes" numeric NOT NULL DEFAULT 019799, "print_node_rate_yards" numeric NOT NULL DEFAULT 1.83, "cut_node_rate_pieces" numeric NOT NULL DEFAULT 0.15, "default_overhead" numeric NOT NULL DEFAULT 0.2, PRIMARY KEY ("id") );COMMENT ON TABLE "flow"."node_rates" IS E'history table with all flat rates - track changes but change slowly. should re-model';
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_node_rates_updated_at"
BEFORE UPDATE ON "flow"."node_rates"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_node_rates_updated_at" ON "flow"."node_rates" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP table "flow"."node_rates";

CREATE TABLE "finance"."node_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "sew_node_rate_minutes" numeric NOT NULL DEFAULT .019799, "print_node_rate_yards" numeric NOT NULL DEFAULT 1.83, "cut_node_rate_pieces" numeric NOT NULL DEFAULT .15, "default_overhead" numeric NOT NULL DEFAULT .2, PRIMARY KEY ("id") );COMMENT ON TABLE "finance"."node_rates" IS E'history table showing changes of rates summary';
CREATE OR REPLACE FUNCTION "finance"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_finance_node_rates_updated_at"
BEFORE UPDATE ON "finance"."node_rates"
FOR EACH ROW
EXECUTE PROCEDURE "finance"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_finance_node_rates_updated_at" ON "finance"."node_rates" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "finance"."node_rates" add column "ended_at" timestamptz
 null;

CREATE TABLE "finance"."material_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "fabric_price" numeric NOT NULL, "ended_at" numeric NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "finance"."material_rates" IS E'material rate tracking history';
CREATE OR REPLACE FUNCTION "finance"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_finance_material_rates_updated_at"
BEFORE UPDATE ON "finance"."material_rates"
FOR EACH ROW
EXECUTE PROCEDURE "finance"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_finance_material_rates_updated_at" ON "finance"."material_rates" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "finance"."material_rates" add column "material_code" text
 not null;

alter table "meta"."bodies" rename column "sewing_estimated_costs" to "estimated_sewing_costs";

alter table "finance"."material_rates" drop column "ended_at" cascade;

alter table "finance"."material_rates" add column "ended_at" timestamptz
 null;

alter table "meta"."bodies" rename column "estimated_sewing_costs" to "estimated_sewing_time";

alter table "meta"."style_sizes" add column "material_usage" jsonb
 null default jsonb_build_array();

alter table "meta"."style_sizes" rename column "material_usage" to "material_usage_statistics";

alter table "meta"."style_sizes" add column "piece_count_checksum" numeric
 null;

ALTER TABLE "meta"."style_sizes" ALTER COLUMN "piece_count_checksum" TYPE int4;

alter table "finance"."material_rates" add column "name" text
 null;

alter table "meta"."bodies" add column "body_piece_count_checksum" integer
 null;
