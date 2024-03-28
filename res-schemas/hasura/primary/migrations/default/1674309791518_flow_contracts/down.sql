
alter table "flow"."contracts" drop constraint "contracts_node_id_fkey";

alter table "flow"."contracts" drop constraint "contracts_owner_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "metadata" jsonb
--  null default jsonb_build_object();

alter table "flow"."contracts" alter column "uri" set not null;

alter table "flow"."contracts" rename column "owner_id" to "owner";

alter table "flow"."contracts" rename column "key" to "code";

alter table "flow"."contracts" rename column "owner" to "owner_user_id";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "status" text
--  not null;

alter table "flow"."users" alter column "node_id" set not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."nodes" add column "type" text
--  null;
