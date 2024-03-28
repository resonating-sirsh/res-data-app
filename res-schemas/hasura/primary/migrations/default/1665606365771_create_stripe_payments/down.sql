
DELETE FROM "sell"."transaction_types" WHERE "value" = 'credit';

DELETE FROM "sell"."transaction_types" WHERE "value" = 'debit';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."transaction_types" add column "comment" text
--  null;

comment on column "sell"."transaction_types"."comment" is E'This represents ';
alter table "sell"."transaction_types" alter column "comment" set default nextval('sell.transaction_types_comment_seq'::regclass);
alter table "sell"."transaction_types" alter column "comment" drop not null;
alter table "sell"."transaction_types" add column "comment" int4;

DROP TABLE "sell"."transaction_types";

DROP TABLE "sell"."transactions";

comment on column "sell"."subscriptions"."active_status" is E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
alter table "sell"."subscriptions"
  add constraint "subscriptions_active_status_fkey"
  foreign key (active_status)
  references "sell"."subscription_status"
  (value) on update set default on delete set default;
alter table "sell"."subscriptions" alter column "active_status" drop not null;
alter table "sell"."subscriptions" add column "active_status" text;

alter table "sell"."subscriptions" drop constraint "subscriptions_collection_method_fkey";

alter table "sell"."collection_methods" rename to "collection_method";

DELETE FROM "sell"."collection_method" WHERE "value" = 'send_invoice';

DELETE FROM "sell"."collection_method" WHERE "value" = 'charge_automatically';

DROP TABLE "sell"."collection_method";

alter table "sell"."subscriptions" drop constraint "subscriptions_active_status_fkey";

DELETE FROM "sell"."subscription_status" WHERE "value" = 'inactive';

DELETE FROM "sell"."subscription_status" WHERE "value" = 'active';

DROP TABLE "sell"."subscription_status";

DROP TABLE "sell"."subscriptions";

DROP TABLE "sell"."stripe_accounts";

alter table "sell"."srtripe_servers" rename to "srtripe_server";

DELETE FROM "sell"."srtripe_server" WHERE "value" = 'production';

DELETE FROM "sell"."srtripe_server" WHERE "value" = 'development';

DROP TABLE "sell"."srtripe_server";
