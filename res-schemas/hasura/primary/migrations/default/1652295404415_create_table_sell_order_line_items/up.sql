CREATE TABLE "sell"."order_line_items" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "order_id" uuid NOT NULL, "source_order_line_item_id" text, "sku" text NOT NULL, "quantity" integer NOT NULL DEFAULT 1, "price" numeric, "source_metadata" jsonb, PRIMARY KEY ("id") , FOREIGN KEY ("order_id") REFERENCES "sell"."orders"("id") ON UPDATE cascade ON DELETE cascade);COMMENT ON TABLE "sell"."order_line_items" IS E'Individual Line items from orders';
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
CREATE TRIGGER "set_sell_order_line_items_updated_at"
BEFORE UPDATE ON "sell"."order_line_items"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_order_line_items_updated_at" ON "sell"."order_line_items" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
