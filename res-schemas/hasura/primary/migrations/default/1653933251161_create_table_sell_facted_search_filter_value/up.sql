CREATE TABLE "sell"."faceted_search_filter_value" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "title" text NOT NULL, "value" text NOT NULL, "filter_id" uuid NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") , FOREIGN KEY ("filter_id") REFERENCES "sell"."faceted_search_filters_collection"("id") ON UPDATE cascade ON DELETE cascade, UNIQUE ("id"));COMMENT ON TABLE "sell"."faceted_search_filter_value" IS E'Values used within a Filter Collection. Ej: Size 1, Size 2 (Filter Collection: Size)';
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
CREATE TRIGGER "set_sell_faceted_search_filter_value_updated_at"
BEFORE UPDATE ON "sell"."faceted_search_filter_value"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_faceted_search_filter_value_updated_at" ON "sell"."faceted_search_filter_value" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
