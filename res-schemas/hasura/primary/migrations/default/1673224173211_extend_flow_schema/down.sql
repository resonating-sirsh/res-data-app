
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "asset_type" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "uri" text
--  not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "requires_contracts" jsonb
--  null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "version" int
--  not null default '0';

alter table "flow"."contracts" alter column "key" drop not null;
alter table "flow"."contracts" add column "key" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "key" text
--  null;

alter table "flow"."nodes" rename column "queue_slack_channel" to "node_queue_slack_channel";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."nodes" add column "node_queue_slack_channel" text
--  null;
