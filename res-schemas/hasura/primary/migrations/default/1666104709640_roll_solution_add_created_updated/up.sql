
alter table "make"."roll_solutions" add column "updated_at" timestamptz
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
CREATE TRIGGER "set_make_roll_solutions_updated_at"
BEFORE UPDATE ON "make"."roll_solutions"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_roll_solutions_updated_at" ON "make"."roll_solutions" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "make"."roll_solutions" add column "created_at" timestamptz
 null default now();

alter table "make"."roll_solutions_nests" add column "created_at" timestamptz
 null default now();

alter table "make"."roll_solutions_nests" add column "updated_at" timestamptz
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
CREATE TRIGGER "set_make_roll_solutions_nests_updated_at"
BEFORE UPDATE ON "make"."roll_solutions_nests"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_roll_solutions_nests_updated_at" ON "make"."roll_solutions_nests" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "make"."material_solutions" add column "created_at" timestamptz
 null default now();

alter table "make"."material_solutions" add column "updated_at" timestamptz
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
CREATE TRIGGER "set_make_material_solutions_updated_at"
BEFORE UPDATE ON "make"."material_solutions"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_material_solutions_updated_at" ON "make"."material_solutions" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
