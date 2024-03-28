CREATE TABLE "make"."make_inventory_checkout" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "metadata" jsonb DEFAULT jsonb_build_object(), "checkout_at" timestamptz NOT NULL, "make_inventory_id" uuid NOT NULL, "fulfillment_item_id" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("make_inventory_id") REFERENCES "make"."make_order_in_inventory"("id") ON UPDATE restrict ON DELETE restrict);
CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_make_make_inventory_checkout_updated_at"
BEFORE UPDATE ON "make"."make_inventory_checkout"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_make_inventory_checkout_updated_at" ON "make"."make_inventory_checkout" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
