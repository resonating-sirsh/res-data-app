
CREATE TABLE "infraestructure"."dead_letters" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, "uri" text NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), PRIMARY KEY ("id") );COMMENT ON TABLE "infraestructure"."dead_letters" IS E'generic dead letter queue pointer to s3 payloads';
CREATE OR REPLACE FUNCTION "infraestructure"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_infraestructure_dead_letters_updated_at"
BEFORE UPDATE ON "infraestructure"."dead_letters"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_dead_letters_updated_at" ON "infraestructure"."dead_letters" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "infraestructure"."dead_letters" add column "reprocessed_date" timestamptz
 null;

comment on column "infraestructure"."dead_letters"."reprocessed_date" is E'When we try to processed the dead letter queue by re-posting a message, we can update this date to say we have tried.';
alter table "infraestructure"."dead_letters" rename column "reprocessed_date" to "reprocessed_at";

alter table "infraestructure"."dead_letters" add column "counter" integer
 null default '0';
