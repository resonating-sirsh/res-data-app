alter table "dxa"."flow_node_run" add column if not exists "auto_retries" integer
 not null default '0';
