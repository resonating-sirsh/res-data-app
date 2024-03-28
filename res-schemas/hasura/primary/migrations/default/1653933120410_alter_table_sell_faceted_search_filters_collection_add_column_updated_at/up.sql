alter table "sell"."faceted_search_filters_collection" add column "updated_at" timestamptz
 null default now();

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
CREATE TRIGGER "set_sell_faceted_search_filters_collection_updated_at"
BEFORE UPDATE ON "sell"."faceted_search_filters_collection"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_faceted_search_filters_collection_updated_at" ON "sell"."faceted_search_filters_collection" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
