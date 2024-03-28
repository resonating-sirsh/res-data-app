CREATE TABLE "make"."one_piece_healing_requests" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "request_id" text NOT NULL, "metadata" jsonb NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "make"."one_piece_healing_requests" IS E'records every unqiue request by unique request if to heal a piece';
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
CREATE TRIGGER "set_make_one_piece_healing_requests_updated_at"
BEFORE UPDATE ON "make"."one_piece_healing_requests"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_piece_healing_requests_updated_at" ON "make"."one_piece_healing_requests" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
