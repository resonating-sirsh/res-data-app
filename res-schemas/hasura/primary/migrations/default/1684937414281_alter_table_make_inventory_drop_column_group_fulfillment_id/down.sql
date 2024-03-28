alter table "make"."inventory" alter column "group_fulfillment_id" drop not null;
alter table "make"."inventory" add column "group_fulfillment_id" text;
