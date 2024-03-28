
CREATE TABLE "infraestructure"."flow_context_runs" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "name" text NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "payloads" jsonb NOT NULL, "task_key" text NOT NULL, "metadata" jsonb NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "infraestructure"."flow_context_runs" IS E'when we run the flow context we may want to save state';
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
CREATE TRIGGER "set_infraestructure_flow_context_runs_updated_at"
BEFORE UPDATE ON "infraestructure"."flow_context_runs"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_flow_context_runs_updated_at" ON "infraestructure"."flow_context_runs" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "infraestructure"."flow_context_runs" add column "parallel_set_ids" jsonb
 null default jsonb_build_object();

alter table "infraestructure"."flow_context_runs" add column "logs" text
 null;

alter table "infraestructure"."flow_context_runs" add column "failed_pods" jsonb
 null default jsonb_build_object();
