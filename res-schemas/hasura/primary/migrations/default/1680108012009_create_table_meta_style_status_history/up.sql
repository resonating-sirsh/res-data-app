CREATE TABLE "meta"."style_status_history" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "updated_at" timestamptz NOT NULL DEFAULT now(), "created_at" timestamptz NOT NULL DEFAULT now(), "style_id" uuid NOT NULL, "contracts_failing" jsonb NOT NULL DEFAULT jsonb_build_object(), "body_id" uuid, "body_version" int NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."style_status_history" IS E'maintains status of styles by body version and what their status is - not a complete log of all states a style goes through';
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
CREATE TRIGGER "set_meta_style_status_history_updated_at"
BEFORE UPDATE ON "meta"."style_status_history"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_style_status_history_updated_at" ON "meta"."style_status_history" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
