
alter table "flow"."nodes" add column "node_queue_slack_channel" text
 null;

alter table "flow"."nodes" rename column "node_queue_slack_channel" to "queue_slack_channel";

alter table "flow"."contracts" add column "key" text
 null;

alter table "flow"."contracts" drop column "key" cascade;

alter table "flow"."contracts" add column "version" int
 not null default '0';

alter table "flow"."contracts" add column "requires_contracts" jsonb
 null default jsonb_build_object();

alter table "flow"."contracts" add column "uri" text
 not null;

alter table "flow"."contracts" add column "asset_type" text
 null;
