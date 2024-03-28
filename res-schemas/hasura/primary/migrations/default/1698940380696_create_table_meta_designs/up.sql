CREATE TABLE "meta"."designs" ("id" uuid NOT NULL, 
"body_code" text NOT NULL, 
"body_version" numeric NOT NULL, 
"brand" text NOT NULL,
"color_code" text NOT NULL, 
"created_at" timestamptz NOT NULL DEFAULT now(), 
"updated_at" timestamptz NOT NULL DEFAULT now(), 
"design" jsonb NOT NULL DEFAULT jsonb_build_object(), 
"body_id" uuid NOT NULL, 
"color_id" uuid NOT NULL, 
PRIMARY KEY ("id") , 
FOREIGN KEY ("body_id") REFERENCES "meta"."bodies"("id") ON UPDATE restrict ON DELETE restrict, 
FOREIGN KEY ("color_id") REFERENCES "meta"."colors"("id") ON UPDATE restrict ON DELETE restrict, UNIQUE ("id"));COMMENT ON TABLE "meta"."designs" IS E'placements, pins, pairs etc connecting artwork to pieces on a body';
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
CREATE TRIGGER "set_meta_designs_updated_at"
BEFORE UPDATE ON "meta"."designs"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_designs_updated_at" ON "meta"."designs" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
