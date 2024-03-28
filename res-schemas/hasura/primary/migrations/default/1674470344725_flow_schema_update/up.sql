

alter table "flow"."contracts" rename column "key" to "code";

alter table "flow"."contracts" drop column "metadata" cascade;

alter table "flow"."users" alter column "slack_uid" drop not null;
