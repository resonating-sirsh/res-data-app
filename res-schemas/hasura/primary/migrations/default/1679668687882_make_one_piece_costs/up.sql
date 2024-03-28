
CREATE TABLE "make"."one_pieces_costs" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "material_consumption" jsonb DEFAULT jsonb_build_object(), "ink_consumption" jsonb DEFAULT jsonb_build_object(), "electricity_consumption" jsonb DEFAULT jsonb_build_object(), "water_consumption" jsonb DEFAULT jsonb_build_object(), PRIMARY KEY ("id") , FOREIGN KEY ("id") REFERENCES "make"."one_pieces"("id") ON UPDATE restrict ON DELETE restrict, UNIQUE ("id"));COMMENT ON TABLE "make"."one_pieces_costs" IS E'This table contains costs data per resource for each one_piece.';
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
CREATE TRIGGER "set_make_one_pieces_costs_updated_at"
BEFORE UPDATE ON "make"."one_pieces_costs"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_pieces_costs_updated_at" ON "make"."one_pieces_costs" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_fkey",
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update set null on delete set null;

alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_fkey",
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update set default on delete set default;

alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_fkey";

alter table "make"."one_pieces_costs" add column "ink_consumption_array" Numeric[]
 null;

ALTER TABLE "make"."one_pieces_costs" ALTER COLUMN "ink_consumption_array" TYPE float8[];

ALTER TABLE "make"."one_pieces_costs" ALTER COLUMN "ink_consumption_array" TYPE float8[];

alter table "make"."one_pieces_costs" add column "test_array" Numeric[]
 null default '{5}';

alter table "make"."one_pieces_costs" drop column "test_array" cascade;

alter table "make"."one_pieces_costs" drop column "ink_consumption_array" cascade;

alter table "make"."one_pieces_costs" add constraint "one_pieces_costs_id_key" unique ("id");

alter table "make"."one_pieces_costs"
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update restrict on delete restrict;
