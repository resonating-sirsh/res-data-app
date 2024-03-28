CREATE TABLE "sell"."orders" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "customer_id" uuid NOT NULL, "ecommerce_source" text NOT NULL, "source_order_id" text, "ordered_at" timestamptz NOT NULL, "brand_code" text NOT NULL, "source_metadata" jsonb NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "sell"."orders" IS E'Tracks orders from all sources';
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
CREATE TRIGGER "set_sell_orders_updated_at"
BEFORE UPDATE ON "sell"."orders"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_orders_updated_at" ON "sell"."orders" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
