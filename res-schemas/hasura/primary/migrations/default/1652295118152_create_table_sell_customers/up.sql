CREATE TABLE "sell"."customers" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "address1" text, "address2" text, "city" text, "state" text, "zipcode" text, "phone" text, "first_name" text, "last_name" text, "email" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("email"));COMMENT ON TABLE "sell"."customers" IS E'All customers, and records for brands to send orders to themselves';
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
CREATE TRIGGER "set_sell_customers_updated_at"
BEFORE UPDATE ON "sell"."customers"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_customers_updated_at" ON "sell"."customers" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
