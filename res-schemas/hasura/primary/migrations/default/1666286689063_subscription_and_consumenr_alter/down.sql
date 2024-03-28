
comment on column "sell"."transactions"."balance_after" is NULL;

comment on column "sell"."transactions"."balance_before" is NULL;

comment on column "sell"."transactions"."amount" is NULL;

comment on column "sell"."subscriptions"."balance" is NULL;

comment on column "sell"."subscriptions"."price_amount" is NULL;

alter table "sell"."subscriptions" drop column "price_amount";
ALTER TABLE "sell"."transactions" ALTER COLUMN "amount" TYPE numeric;

ALTER TABLE "sell"."transactions" ALTER COLUMN "balance_after" TYPE numeric;

ALTER TABLE "sell"."transactions" ALTER COLUMN "balance_before" TYPE numeric;

ALTER TABLE "sell"."subscriptions" ALTER COLUMN "balance" drop default;
ALTER TABLE "sell"."subscriptions" ALTER COLUMN "balance" TYPE numeric;

alter table "sell"."subscriptions" drop column "current_period_end";
alter table "sell"."subscriptions" drop column "current_period_start";
comment on column "sell"."subscriptions"."next_invoice" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions" alter column "next_invoice" drop not null;
alter table "sell"."subscriptions" add column "next_invoice" date;

comment on column "sell"."subscriptions"."last_payment_at" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions" alter column "last_payment_at" drop not null;
alter table "sell"."subscriptions" add column "last_payment_at" timestamptz;

alter table "sell"."transactions" drop constraint "transactions_currency_fkey";

alter table "sell"."subscriptions" drop constraint "subscriptions_currency_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "currency" text
--  not null;

DELETE FROM "sell"."currencies" WHERE "value" = 'usd';

DROP TABLE "sell"."currencies";

alter table "sell"."subscriptions" alter column "end_date" set not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "sell"."stripe_accounts";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."transactions" add column "description" text
--  not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."transactions" add column "currency" text
--  not null;

comment on column "sell"."subscriptions"."account" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions" alter column "account" drop not null;
alter table "sell"."subscriptions" add column "account" int4;

alter table "sell"."subscriptions" drop constraint "subscriptions_pkey";
alter table "sell"."subscriptions"
    add constraint "subscriptions_pkey"
    primary key ("account");

alter table "sell"."subscriptions"
  add constraint "subscriptions_account_fkey"
  foreign key ("account")
  references "sell"."stripe_accounts"
  ("id") on update restrict on delete restrict;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "brand_code" text
--  not null unique;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "payment_method" text
--  not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "brand_id" text
--  not null unique;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."subscriptions" add column "stripe_customer_id" text
--  not null unique;
