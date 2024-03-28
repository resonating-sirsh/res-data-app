
alter table "sell"."stripe_accounts" add column "payment_method" text
 not null default '0';

alter table "sell"."subscriptions" add column "price" text
 not null;

alter table "sell"."subscriptions" drop column "payment_history" cascade;
