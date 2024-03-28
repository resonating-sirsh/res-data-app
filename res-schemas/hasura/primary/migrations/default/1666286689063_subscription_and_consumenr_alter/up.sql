
alter table "sell"."subscriptions" add column "stripe_customer_id" text
 not null unique;

alter table "sell"."subscriptions" add column "brand_id" text
 not null unique;

alter table "sell"."subscriptions" add column "payment_method" text
 not null;

alter table "sell"."subscriptions" add column "brand_code" text
 not null unique;

alter table "sell"."subscriptions" drop constraint "subscriptions_account_fkey";

BEGIN TRANSACTION;
ALTER TABLE "sell"."subscriptions" DROP CONSTRAINT "subscriptions_pkey";

ALTER TABLE "sell"."subscriptions"
    ADD CONSTRAINT "subscriptions_pkey" PRIMARY KEY ("id");
COMMIT TRANSACTION;

alter table "sell"."subscriptions" drop column "account" cascade;

alter table "sell"."transactions" add column "currency" text
 not null;

alter table "sell"."transactions" add column "description" text
 not null;

DROP table "sell"."stripe_accounts";

alter table "sell"."subscriptions" alter column "end_date" drop not null;

CREATE TABLE "sell"."currencies" ("value" text NOT NULL, "comment" text NOT NULL, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."currencies" IS E'Type of currencies available. Only usd for now. ';

INSERT INTO "sell"."currencies"("value", "comment") VALUES (E'usd', E'United States Dollar');

alter table "sell"."subscriptions" add column "currency" text
 not null;

alter table "sell"."subscriptions"
  add constraint "subscriptions_currency_fkey"
  foreign key ("currency")
  references "sell"."currencies"
  ("value") on update set default on delete set default;

alter table "sell"."transactions"
  add constraint "transactions_currency_fkey"
  foreign key ("currency")
  references "sell"."currencies"
  ("value") on update set default on delete set default;

alter table "sell"."subscriptions" drop column "last_payment_at" cascade;

alter table "sell"."subscriptions" drop column "next_invoice" cascade;

alter table "sell"."subscriptions" add column "current_period_start" timestamptz
 not null;

alter table "sell"."subscriptions" add column "current_period_end" timestamptz
 not null;

ALTER TABLE "sell"."subscriptions" ALTER COLUMN "balance" TYPE int4;
alter table "sell"."subscriptions" alter column "balance" set default '0';

ALTER TABLE "sell"."transactions" ALTER COLUMN "balance_before" TYPE int4;

ALTER TABLE "sell"."transactions" ALTER COLUMN "balance_after" TYPE int4;

ALTER TABLE "sell"."transactions" ALTER COLUMN "amount" TYPE int4;

alter table "sell"."subscriptions" add column "price_amount" integer
 not null;

comment on column "sell"."subscriptions"."price_amount" is E'In cents';

comment on column "sell"."subscriptions"."balance" is E'In cents.';

comment on column "sell"."transactions"."amount" is E'In cents.';

comment on column "sell"."transactions"."balance_before" is E'In cents.';

comment on column "sell"."transactions"."balance_after" is E'In cents.';
