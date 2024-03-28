CREATE TABLE "infraestructure"."bridge_sku_one_counter" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "cancelled_at" timestamptz, "one_number" text NOT NULL, "sku" text NOT NULL, "style_sku" text NOT NULL, "make_sku_increment" integer NOT NULL, PRIMARY KEY ("id") );
CREATE OR REPLACE FUNCTION "infraestructure"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_infraestructure_bridge_sku_one_counter_updated_at"
BEFORE UPDATE ON "infraestructure"."bridge_sku_one_counter"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_bridge_sku_one_counter_updated_at" ON "infraestructure"."bridge_sku_one_counter" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
