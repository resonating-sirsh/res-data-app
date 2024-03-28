
alter table "infraestructure"."res_notifications" add column "notification_id" text
 null;

alter table "infraestructure"."res_notifications" add column "payload" json
 null;

alter table "infraestructure"."res_notifications" alter column "source_table_id" drop not null;
