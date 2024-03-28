CREATE TABLE "infraestructure"."res_notifications" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "topic" text NOT NULL, "destination" text NOT NULL, "source" text NOT NULL, "message" text NOT NULL, "links" json, "channels" json NOT NULL, "subchannel" text NOT NULL, "received" boolean NOT NULL DEFAULT false, "read" boolean NOT NULL DEFAULT false, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz DEFAULT now(), "read_at" timestamptz, PRIMARY KEY ("id") , UNIQUE ("id"));
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
CREATE TRIGGER "set_infraestructure_res_notifications_updated_at"
BEFORE UPDATE ON "infraestructure"."res_notifications"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_res_notifications_updated_at" ON "infraestructure"."res_notifications" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
