
comment on column "sell"."subscriptions"."payment_history" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions" alter column "payment_history" drop not null;
alter table "sell"."subscriptions" add column "payment_history" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "price" text
--  not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."stripe_accounts" add column "payment_method" text
--  not null default '0';
