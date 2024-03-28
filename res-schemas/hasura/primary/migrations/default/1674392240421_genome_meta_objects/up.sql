
CREATE TABLE "meta"."piece_components" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "key" text NOT NULL, "type" text NOT NULL, "sew_symbol" text, "commercial_acceptability_zone" text, PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."piece_components" IS E'the piece names are broken down into components';
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
CREATE TRIGGER "set_meta_piece_components_updated_at"
BEFORE UPDATE ON "meta"."piece_components"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_piece_components_updated_at" ON "meta"."piece_components" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "meta"."piece_names" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "key" text NOT NULL, "name" text NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."piece_names" IS E'a list of all known piece names';
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
CREATE TRIGGER "set_meta_piece_names_updated_at"
BEFORE UPDATE ON "meta"."piece_names"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_piece_names_updated_at" ON "meta"."piece_names" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "meta"."sizes" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "key" text NOT NULL, "size_chart" text NOT NULL, "size_normalized" text NOT NULL, PRIMARY KEY ("id") );
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
CREATE TRIGGER "set_meta_sizes_updated_at"
BEFORE UPDATE ON "meta"."sizes"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_sizes_updated_at" ON "meta"."sizes" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "meta"."materials" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "ket" text NOT NULL, "compensation_length" numeric NOT NULL, "compensation_width" numeric NOT NULL, "paper_marker_compensation_length" numeric NOT NULL, "paper_marker_compensation_width" numeric NOT NULL, "fabric_type" text, "material_stability" text, "cuttable_width_inches" numeric, "offset_size_inches" numeric NOT NULL, PRIMARY KEY ("id") );
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
CREATE TRIGGER "set_meta_materials_updated_at"
BEFORE UPDATE ON "meta"."materials"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_materials_updated_at" ON "meta"."materials" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "meta"."materials" rename column "ket" to "key";

alter table "meta"."materials" add column "material_taxonomy" text
 null;

alter table "meta"."materials" add column "pretreatment_type" text
 null;

alter table "meta"."materials" alter column "offset_size_inches" drop not null;
