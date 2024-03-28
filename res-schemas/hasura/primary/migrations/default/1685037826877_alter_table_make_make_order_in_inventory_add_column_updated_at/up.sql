alter table "make"."make_order_in_inventory" add column "updated_at" timestamptz
 null default now();

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
CREATE TRIGGER "set_make_make_order_in_inventory_updated_at"
BEFORE UPDATE ON "make"."make_order_in_inventory"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_make_order_in_inventory_updated_at" ON "make"."make_order_in_inventory" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
