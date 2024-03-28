CREATE TABLE "flow"."finance_node_rates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "ended_at" timestamptz, "sew_rate_minutes" numeric NOT NULL, "print_rate_yards" numeric NOT NULL, "cut_rate_units" numeric NOT NULL, "default_overhead" numeric NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "flow"."finance_node_rates" IS E'a placeholder for storing rates snapshots until some more sophisticated comes along';
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
CREATE TRIGGER "set_flow_finance_node_rates_updated_at"
BEFORE UPDATE ON "flow"."finance_node_rates"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_finance_node_rates_updated_at" ON "flow"."finance_node_rates" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
