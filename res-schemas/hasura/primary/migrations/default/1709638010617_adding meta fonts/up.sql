
CREATE TABLE "meta"."fonts" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(), 
  "created_at" timestamptz NOT NULL DEFAULT now(), 
  "updated_at" timestamptz NOT NULL DEFAULT now(), 
  "name" text NOT NULL, 
  "brand" text not null,
  "uri" text not null,
  "source" text NOT NULL,
  "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), 
  "deleted_at" timestamptz NULL,
  PRIMARY KEY ("id") );
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
CREATE TRIGGER "set_meta_fonts_updated_at"
BEFORE UPDATE ON "meta"."fonts"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_fonts_updated_at" ON "meta"."fonts" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

