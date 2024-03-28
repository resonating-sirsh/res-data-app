
alter table "sell"."stripe_servers" rename to "srtripe_servers";


alter table "sell"."transactions" drop constraint "transactions_subscription_fkey",
  add constraint "transactions_subscription_fkey"
  foreign key ("subscription")
  references "sell"."subscriptions"
  ("id") on update set default on delete set default;

alter table "sell"."transactions" drop constraint "transactions_currency_fkey",
  add constraint "transactions_currency_fkey"
  foreign key ("currency")
  references "sell"."currencies"
  ("value") on update set default on delete set default;

alter table "sell"."subscriptions" drop constraint "subscriptions_collection_method_fkey",
  add constraint "subscriptions_collection_method_fkey"
  foreign key ("brand_id")
  references "sell"."brands"
  ("id") on update restrict on delete restrict;

alter table "sell"."subscriptions" drop constraint "subscriptions_brand_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "brand_id" integer
--  not null unique;

comment on column "sell"."subscriptions"."brand_code" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions" add constraint "subscriptions_brand_code_key" unique (brand_code);
alter table "sell"."subscriptions" alter column "brand_code" drop not null;
alter table "sell"."subscriptions" add column "brand_code" text;

comment on column "sell"."subscriptions"."brand_id" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions" add constraint "subscriptions_brand_id_key" unique (brand_id);
alter table "sell"."subscriptions" alter column "brand_id" drop not null;
alter table "sell"."subscriptions" add column "brand_id" text;

DROP TABLE "sell"."brands";
