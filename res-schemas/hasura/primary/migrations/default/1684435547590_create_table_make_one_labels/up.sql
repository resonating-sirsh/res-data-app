CREATE TABLE "make"."one_labels" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "code" text NOT NULL, "oid" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("oid") REFERENCES "make"."one_orders"("oid") ON UPDATE no action ON DELETE no action);COMMENT ON TABLE "make"."one_labels" IS E'a lookup for one code experience linked back to ones';
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
CREATE TRIGGER "set_make_one_labels_updated_at"
BEFORE UPDATE ON "make"."one_labels"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_labels_updated_at" ON "make"."one_labels" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
