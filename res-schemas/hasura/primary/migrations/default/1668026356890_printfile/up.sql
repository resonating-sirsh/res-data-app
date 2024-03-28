
CREATE TABLE "make"."printfile" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, "roll_name" text NOT NULL, "s3_path" text NOT NULL, "width_px" integer NOT NULL, "height_px" integer NOT NULL, "width_in" float4 NOT NULL, "height_in" float4 NOT NULL, "airtable_record_id" text, PRIMARY KEY ("id") , UNIQUE ("name"));
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
CREATE TRIGGER "set_make_printfile_updated_at"
BEFORE UPDATE ON "make"."printfile"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_printfile_updated_at" ON "make"."printfile" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "make"."printfile_pieces" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "printfile_id" uuid NOT NULL, "piece_id" text NOT NULL, "min_y_in" float4 NOT NULL, "max_y_in" float4 NOT NULL, "min_x_in" float4 NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("printfile_id") REFERENCES "make"."printfile"("id") ON UPDATE restrict ON DELETE restrict, FOREIGN KEY ("piece_id") REFERENCES "make"."piece_instances"("id") ON UPDATE restrict ON DELETE restrict, UNIQUE ("printfile_id", "piece_id"));
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
CREATE TRIGGER "set_make_printfile_pieces_updated_at"
BEFORE UPDATE ON "make"."printfile_pieces"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_printfile_pieces_updated_at" ON "make"."printfile_pieces" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."printfile_pieces" add column "metadata" jsonb
 null;

alter table "make"."printfile" add column "metadata" jsonb
 null;
