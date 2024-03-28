
CREATE TABLE "make"."roll_inspection_progress" ("id" serial NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "roll_key" text NOT NULL, "inspector" integer NOT NULL, "started_ts" timestamptz NOT NULL, "finished_ts" timestamptz NOT NULL, "synced_ts" timestamptz NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));
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
CREATE TRIGGER "set_make_roll_inspection_progress_updated_at"
BEFORE UPDATE ON "make"."roll_inspection_progress"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_roll_inspection_progress_updated_at" ON "make"."roll_inspection_progress" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

BEGIN TRANSACTION;
ALTER TABLE "make"."roll_inspection_progress" DROP CONSTRAINT "roll_inspection_progress_pkey";

ALTER TABLE "make"."roll_inspection_progress"
    ADD CONSTRAINT "roll_inspection_progress_pkey" PRIMARY KEY ("roll_key", "inspector");
COMMIT TRANSACTION;

alter table "make"."roll_inspection_progress" alter column "finished_ts" drop not null;

alter table "make"."roll_inspection_progress" alter column "synced_ts" drop not null;
