
CREATE TABLE "make"."machine_configs" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, "node" text, "is_active" Boolean NOT NULL DEFAULT false, "configs" jsonb NOT NULL DEFAULT jsonb_build_object(), "description" text, PRIMARY KEY ("id") , UNIQUE ("name"), UNIQUE ("id"));COMMENT ON TABLE "make"."machine_configs" IS E'This table stores factory\'s machine configurations';
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
CREATE TRIGGER "set_make_machine_configs_updated_at"
BEFORE UPDATE ON "make"."machine_configs"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_machine_configs_updated_at" ON "make"."machine_configs" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."machine_configs" add column "serial_number" Text
 null unique;

alter table "make"."machine_configs" alter column "serial_number" set not null;

alter table "make"."machine_configs" add column "metadata" jsonb
 not null default jsonb_build_object();
