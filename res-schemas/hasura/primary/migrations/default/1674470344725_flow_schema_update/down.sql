
alter table "flow"."users" alter column "slack_uid" set not null;

alter table "flow"."contracts" alter column "metadata" set default jsonb_build_object();
alter table "flow"."contracts" alter column "metadata" drop not null;
alter table "flow"."contracts" add column "metadata" jsonb;

alter table "flow"."contracts" rename column "code" to "key";

alter table "flow"."nodes" alter column "type" drop not null;
alter table "flow"."nodes" add column "type" text;
