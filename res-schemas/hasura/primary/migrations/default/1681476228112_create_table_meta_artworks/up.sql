CREATE TABLE IF NOT EXISTS "meta"."artworks" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(), 
  "name" text NOT NULL, 
  "brand" Text NOT NULL, 
  "description" text, 
  "original_uri" Text NOT NULL, 
  "dpi_300_uri" Text NOT NULL, 
  "dpi_72_uri" Text NOT NULL, 
  "dpi_36_uri" Text NOT NULL, 
  "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), 
  "created_at" timestamptz NOT NULL DEFAULT now(), 
  "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."artworks" IS E'Artworks and downsamples';
CREATE OR REPLACE FUNCTION "meta"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS "set_meta_artworks_updated_at" ON "meta"."artworks";
CREATE TRIGGER "set_meta_artworks_updated_at"
BEFORE UPDATE ON "meta"."artworks"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_artworks_updated_at" ON "meta"."artworks" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
