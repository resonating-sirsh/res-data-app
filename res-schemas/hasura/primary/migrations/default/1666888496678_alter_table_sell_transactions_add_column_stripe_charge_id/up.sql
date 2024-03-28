alter table "sell"."transactions" add column "stripe_charge_id" text
 not null unique;
