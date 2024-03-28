CREATE TABLE "sell"."faceted_search_brand_customization" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "brand_code" text NOT NULL, "changes" json, "filter_id" uuid NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") , FOREIGN KEY ("filter_id") REFERENCES "sell"."faceted_search_filters_collection"("id") ON UPDATE cascade ON DELETE cascade, UNIQUE ("id"));COMMENT ON TABLE "sell"."faceted_search_brand_customization" IS E'Changes made by the brand to the Faceted Search';
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
CREATE TRIGGER "set_sell_faceted_search_brand_customization_updated_at"
BEFORE UPDATE ON "sell"."faceted_search_brand_customization"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_faceted_search_brand_customization_updated_at" ON "sell"."faceted_search_brand_customization" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
