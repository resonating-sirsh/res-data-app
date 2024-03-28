
alter table "make"."nests" add column "created_at" timestamptz
 null default now();

alter table "make"."nests" add column "updated_at" timestamptz
 null default now();

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
CREATE TRIGGER "set_make_nests_updated_at"
BEFORE UPDATE ON "make"."nests"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_nests_updated_at" ON "make"."nests" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
