
alter table "meta"."brand_body_requests" drop column "sketch_id" cascade;

alter table "meta"."brand_body_requests" drop column "annotations" cascade;

alter table "meta"."brand_body_requests" drop column "cover_image_ids" cascade;

alter table "meta"."brand_body_requests" rename column "body_version_id" to "meta_body_id";

alter table "meta"."brand_body_requests"
  add constraint "brand_body_requests_meta_body_id_fkey"
  foreign key ("meta_body_id")
  references "meta"."bodies"
  ("id") on update restrict on delete set null;

alter table "meta"."brand_body_requests" drop constraint "brand_body_requests_meta_body_id_fkey",
  add constraint "brand_body_requests_meta_body_id_fkey"
  foreign key ("meta_body_id")
  references "meta"."bodies"
  ("id") on update cascade on delete set null;

CREATE TABLE "meta"."body_request_assets" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "deleted_at" timestamptz, "name" text NOT NULL, "uri" text NOT NULL, "type" text NOT NULL, "brand_body_request_id" uuid NOT NULL, "metadata" jsonb NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("brand_body_request_id") REFERENCES "meta"."brand_body_requests"("id") ON UPDATE cascade ON DELETE cascade);COMMENT ON TABLE "meta"."body_request_assets" IS E'Assets use on the Body Onboarding process';
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
CREATE TRIGGER "set_meta_body_request_assets_updated_at"
BEFORE UPDATE ON "meta"."body_request_assets"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_body_request_assets_updated_at" ON "meta"."body_request_assets" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "meta"."body_request_asset_nodes" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "asset_id" uuid NOT NULL, "label" text, "type" text NOT NULL, "icon" text, "coordinate_x" Numeric NOT NULL, "coordinate_y" numeric NOT NULL, "coordinate_z" Numeric, PRIMARY KEY ("id") , FOREIGN KEY ("asset_id") REFERENCES "meta"."body_request_assets"("id") ON UPDATE cascade ON DELETE cascade);COMMENT ON TABLE "meta"."body_request_asset_nodes" IS E'Nodes that can be use to ping positions of Assets in the 3D or 2D assets';
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
CREATE TRIGGER "set_meta_body_request_asset_nodes_updated_at"
BEFORE UPDATE ON "meta"."body_request_asset_nodes"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_body_request_asset_nodes_updated_at" ON "meta"."body_request_asset_nodes" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "meta"."body_request_asset_annotations" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "deleted_at" timestamptz, "node_id" uuid NOT NULL, "message" text NOT NULL, "created_by" Text NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("node_id") REFERENCES "meta"."body_request_asset_nodes"("id") ON UPDATE cascade ON DELETE cascade, UNIQUE ("id"));COMMENT ON TABLE "meta"."body_request_asset_annotations" IS E'Comment, Measurment and orther inputs made by the user to annotate the asset on the Body Request';
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
CREATE TRIGGER "set_meta_body_request_asset_annotations_updated_at"
BEFORE UPDATE ON "meta"."body_request_asset_annotations"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_body_request_asset_annotations_updated_at" ON "meta"."body_request_asset_annotations" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "meta"."brand_body_request_asset_trims" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "deleted_at" timestamptz, "name" text, "type" text, "trim_id" text, PRIMARY KEY ("id") , UNIQUE ("id"));
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
CREATE TRIGGER "set_meta_brand_body_request_asset_trims_updated_at"
BEFORE UPDATE ON "meta"."brand_body_request_asset_trims"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_brand_body_request_asset_trims_updated_at" ON "meta"."brand_body_request_asset_trims" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "meta"."brand_body_request_asset_trims" add column "node_id" uuid
 null;

alter table "meta"."brand_body_request_asset_trims"
  add constraint "brand_body_request_asset_trims_node_id_fkey"
  foreign key ("node_id")
  references "meta"."body_request_asset_nodes"
  ("id") on update cascade on delete cascade;
