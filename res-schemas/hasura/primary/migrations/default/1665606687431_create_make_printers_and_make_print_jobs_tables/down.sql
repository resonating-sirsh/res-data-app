
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."printers" add column "metadata" jsonb
--  not null default jsonb_build_object();

alter table "make"."print_jobs" drop constraint "print_jobs_job_id_printer_name_key";

DROP TABLE "make"."print_jobs";

DROP TABLE "make"."printers";
