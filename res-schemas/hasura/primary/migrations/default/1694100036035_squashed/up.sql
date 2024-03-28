CREATE TABLE "meta"."brand_body_requests"(
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  "deleted_at" timestamptz,
  "request_type" text NOT NULL,
  "status" text NOT NULL,
  "body_code" text,
  "body_version_id" uuid,
  "name" text NOT NULL,
  "body_category_id" text,
  "fit_avatar_id" text,
  "brand_code" text NOT NULL,
  "sketch_id" text,
  "cover_image_ids" jsonb,
  "material_onboarding_ids" jsonb,
  "annotations" jsonb NULL,
  "metadata" jsonb NULL,
  PRIMARY KEY ("id"),
  UNIQUE ("id")
);

COMMENT ON TABLE "meta"."brand_body_requests" IS E'Brand intake for create/modify a body from existing or from scratch';

CREATE OR REPLACE FUNCTION "meta"."set_current_timestamp_updated_at"()
  RETURNS TRIGGER
  AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER "set_meta_brand_body_requests_updated_at"
  BEFORE UPDATE ON "meta"."brand_body_requests"
  FOR EACH ROW
  EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();

COMMENT ON TRIGGER "set_meta_brand_body_requests_updated_at" ON "meta"."brand_body_requests" IS 'trigger to set value of column "updated_at" to current timestamp on row update';

CREATE EXTENSION IF NOT EXISTS pgcrypto;

