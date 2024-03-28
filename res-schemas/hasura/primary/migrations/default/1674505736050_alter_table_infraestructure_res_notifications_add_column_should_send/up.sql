alter table "infraestructure"."res_notifications" add column "should_send" boolean
 not null default 'true';
