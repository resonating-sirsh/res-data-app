
CREATE TABLE "meta"."body_pieces_with_material" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "piece_map" jsonb NOT NULL, "sewing_time" integer NOT NULL, "material_code" text NOT NULL, "material_compensation_x" numeric NOT NULL, "material_compensation_y" numeric NOT NULL, "trim_costs" numeric NOT NULL, "nesting_statistics" jsonb NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), "overhead_rate" numeric, "body_code" text NOT NULL, "body_version" integer NOT NULL, "offset_buffer_inches" numeric NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."body_pieces_with_material" IS E'for a particular set of pieces and a material combination, compute some things';
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
CREATE TRIGGER "set_meta_body_pieces_with_material_updated_at"
BEFORE UPDATE ON "meta"."body_pieces_with_material"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_body_pieces_with_material_updated_at" ON "meta"."body_pieces_with_material" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "meta"."body_pieces_with_material" add column "pieces_area_raw" numeric
 not null;

alter table "meta"."body_pieces_with_material" rename column "offset_buffer_inches" to "material_offset_buffer_inches";

alter table "meta"."body_pieces_with_material" drop column "material_code" cascade;

alter table "meta"."body_pieces_with_material" drop column "material_offset_buffer_inches" cascade;

alter table "meta"."body_pieces_with_material" drop column "pieces_area_raw" cascade;

alter table "meta"."body_pieces_with_material" drop column "material_compensation_x" cascade;

alter table "meta"."body_pieces_with_material" drop column "material_compensation_y" cascade;

alter table "meta"."body_pieces_with_material" add column "number_of_materials" integer
 not null;

comment on column "meta"."body_pieces_with_material"."id" is E'this is a hash of the body code, version and piece map';

alter table "meta"."body_pieces_with_material" add column "size_code" text
 not null;

comment on column "meta"."body_pieces_with_material"."id" is E'this is a hash of the body code, version,size_code and piece map';

comment on column "meta"."body_pieces_with_material"."id" is E'this is a hash of the body code, version,size_code and piece map - all this info is in the size piece code so saving a hash of those captures it';

ALTER TABLE "meta"."body_pieces_with_material" ALTER COLUMN "sewing_time" TYPE numeric;
