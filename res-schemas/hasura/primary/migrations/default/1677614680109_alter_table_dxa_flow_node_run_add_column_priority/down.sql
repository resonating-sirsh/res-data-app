-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "dxa"."flow_node_run" add column "priority" integer
--  not null default '0';
alter table "dxa"."flow_node_run" drop column "priority"