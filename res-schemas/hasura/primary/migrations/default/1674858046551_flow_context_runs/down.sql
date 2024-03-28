
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."flow_context_runs" add column "failed_pods" jsonb
--  null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."flow_context_runs" add column "logs" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."flow_context_runs" add column "parallel_set_ids" jsonb
--  null default jsonb_build_object();

DROP TABLE "infraestructure"."flow_context_runs";
