CREATE TABLE "make"."roll_inspection" ("created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "piece_id" text NOT NULL, "nest_key" Text NOT NULL, "inspector" integer NOT NULL, "defect_id" integer NOT NULL, "defect_name" Text, "challenge" boolean, "fail" boolean NOT NULL, PRIMARY KEY ("piece_id","nest_key","inspector") , FOREIGN KEY ("piece_id") REFERENCES "make"."piece_instances"("id") ON UPDATE restrict ON DELETE restrict);
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
CREATE TRIGGER "set_make_roll_inspection_updated_at"
BEFORE UPDATE ON "make"."roll_inspection"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_roll_inspection_updated_at" ON "make"."roll_inspection" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
