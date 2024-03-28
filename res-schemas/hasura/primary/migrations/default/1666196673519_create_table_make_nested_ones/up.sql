
CREATE TABLE "make"."nested_ones" ("id" uuid NOT NULL, "piece_set_key" text NOT NULL, "piece_count" integer NOT NULL, "height_nest_px" integer NOT NULL, "width_nest_px" integer NOT NULL, "utilization" float4 NOT NULL, "material_code" text NOT NULL, "nest_file_path" text NOT NULL, "one_number" integer NOT NULL, "piece_type" text NOT NULL, "metadata" jsonb NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") );
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
CREATE TRIGGER "set_make_nested_ones_updated_at"
BEFORE UPDATE ON "make"."nested_ones"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_nested_ones_updated_at" ON "make"."nested_ones" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "make"."nested_ones" alter column "id" set default gen_random_uuid();



