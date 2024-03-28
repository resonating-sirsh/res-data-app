


alter table "meta"."bodies" add column "profile" text
 not null default 'default';

CREATE TABLE "meta"."body_pieces" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(), 
  "created_at" timestamptz NOT NULL DEFAULT now(), 
  "updated_at" timestamptz NOT NULL DEFAULT now(), 
  "key" text NOT NULL, 
  "type" text NOT NULL, 
  "outer_geojson" jsonb NOT NULL,
  "inner_geojson" jsonb NOT NULL,
  "outer_edges_geojson" jsonb NOT NULL,
  "inner_edges_geojson" jsonb NOT NULL,
  "outer_corners_geojson" jsonb NOT NULL,
  "outer_notches_geojson" jsonb NOT NULL,
  "seam_guides_geojson" jsonb,
  "internal_lines_geojson" jsonb,
  "symmetry" text,
  "grain_line_degrees" integer,
  "size_code" text,
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
CREATE TRIGGER "set_meta_body_pieces_updated_at"
BEFORE UPDATE ON "meta"."body_pieces"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_body_pieces_updated_at" ON "meta"."body_pieces" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;


alter table "meta"."body_pieces" add column "body_id" uuid
 null default gen_random_uuid();

alter table "meta"."body_pieces" alter column "body_id" set not null;

alter table "meta"."body_pieces"
  add constraint "body_pieces_body_id_fkey"
  foreign key ("body_id")
  references "meta"."bodies"
  ("id") on update restrict on delete restrict;

alter table "meta"."body_pieces" rename column "size_code" to "vs_size_code";

alter table "meta"."bodies" rename column "modified_at" to "updated_at";

alter table "meta"."body_pieces" add column "size_code" text
 null;

alter table "meta"."body_pieces" add constraint "body_pieces_key_key" unique ("key");

alter table "meta"."body_pieces" add column "vs_key" text
 not null;

alter table "meta"."body_pieces" add column "metadata" jsonb
 null;
