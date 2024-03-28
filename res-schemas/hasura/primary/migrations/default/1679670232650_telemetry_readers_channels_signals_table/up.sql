
create schema "premises";

CREATE TABLE "premises"."rfid_readers" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "serial_number" text, "name" text NOT NULL, "is_active" boolean NOT NULL DEFAULT False, PRIMARY KEY ("id") , UNIQUE ("id"), UNIQUE ("serial_number"));COMMENT ON TABLE "premises"."rfid_readers" IS E'This table contains information about every RFID reader in the factory';
CREATE OR REPLACE FUNCTION "premises"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_premises_rfid_readers_updated_at"
BEFORE UPDATE ON "premises"."rfid_readers"
FOR EACH ROW
EXECUTE PROCEDURE "premises"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_premises_rfid_readers_updated_at" ON "premises"."rfid_readers" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "premises"."channels" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "channel_id" text NOT NULL, "is_active" boolean NOT NULL DEFAULT False, "location" text, PRIMARY KEY ("id") , UNIQUE ("id"));
CREATE OR REPLACE FUNCTION "premises"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_premises_channels_updated_at"
BEFORE UPDATE ON "premises"."channels"
FOR EACH ROW
EXECUTE PROCEDURE "premises"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_premises_channels_updated_at" ON "premises"."channels" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "premises"."channels" rename to "rfid_channels";

CREATE TABLE "premises"."rfid_signals" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "node" text, "description" text, "signal_name" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"), UNIQUE ("signal_name"));
CREATE OR REPLACE FUNCTION "premises"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_premises_rfid_signals_updated_at"
BEFORE UPDATE ON "premises"."rfid_signals"
FOR EACH ROW
EXECUTE PROCEDURE "premises"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_premises_rfid_signals_updated_at" ON "premises"."rfid_signals" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "premises"."rfid_readers"
  add constraint "rfid_readers_id_fkey"
  foreign key ("id")
  references "premises"."rfid_channels"
  ("id") on update restrict on delete restrict;

alter table "premises"."rfid_channels"
  add constraint "rfid_channels_id_fkey"
  foreign key ("id")
  references "premises"."rfid_signals"
  ("id") on update restrict on delete restrict;
