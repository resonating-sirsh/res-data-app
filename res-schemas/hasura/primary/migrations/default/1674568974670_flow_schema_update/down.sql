
alter table "flow"."contracts" alter column "metadata" set default jsonb_build_object();
alter table "flow"."contracts" alter column "metadata" drop not null;
alter table "flow"."contracts" add column "metadata" jsonb;

-- alter table "flow"."contracts" alter column "key" drop not null;
-- alter table "flow"."contracts" add column "key" text;


alter table "flow"."contracts" rename column "key" to "code";

alter table "flow"."contracts" alter column "key" drop not null;
alter table "flow"."contracts" add column "key" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "metadata" jsonb
--  null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "key" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."nodes" add column "type" text
--  null;
