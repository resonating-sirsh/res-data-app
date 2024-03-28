
CREATE TABLE "make"."printers" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("name"));COMMENT ON TABLE "make"."printers" IS E'Storing information about printers';
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
CREATE TRIGGER "set_make_printers_updated_at"
BEFORE UPDATE ON "make"."printers"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_printers_updated_at" ON "make"."printers" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "make"."print_jobs" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "printer_name" text NOT NULL, "printer_id" uuid NOT NULL, "status" text NOT NULL, "start_time" timestamptz NOT NULL, "end_time" timestamptz, "job_id" text NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), PRIMARY KEY ("id") , FOREIGN KEY ("printer_id") REFERENCES "make"."printers"("id") ON UPDATE restrict ON DELETE restrict, FOREIGN KEY ("printer_name") REFERENCES "make"."printers"("name") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "make"."print_jobs" IS E'Storing data from print job data kafka topic (res_premises.printer_data_collector.print_job_data)';
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
CREATE TRIGGER "set_make_print_jobs_updated_at"
BEFORE UPDATE ON "make"."print_jobs"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_print_jobs_updated_at" ON "make"."print_jobs" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."print_jobs" add constraint "print_jobs_job_id_printer_name_key" unique ("job_id", "printer_name");

alter table "make"."printers" add column "metadata" jsonb
 not null default jsonb_build_object();
