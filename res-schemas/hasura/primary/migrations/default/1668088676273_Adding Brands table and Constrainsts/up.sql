

CREATE TABLE "sell"."brands" ("id" serial NOT NULL, "airtable_brand_id" text NOT NULL, "airtable_brand_code" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"), UNIQUE ("airtable_brand_id"), UNIQUE ("airtable_brand_code"));COMMENT ON TABLE "sell"."brands" IS E'These are Resonance Brands.';

alter table "sell"."subscriptions" drop column "brand_id" cascade;

alter table "sell"."subscriptions" drop column "brand_code" cascade;

alter table "sell"."subscriptions" add column "brand_id" integer
 not null unique;

alter table "sell"."subscriptions"
  add constraint "subscriptions_brand_id_fkey"
  foreign key ("brand_id")
  references "sell"."brands"
  ("id") on update restrict on delete restrict;

alter table "sell"."subscriptions" drop constraint "subscriptions_collection_method_fkey",
  add constraint "subscriptions_collection_method_fkey"
  foreign key ("collection_method")
  references "sell"."collection_methods"
  ("value") on update restrict on delete restrict;

alter table "sell"."transactions" drop constraint "transactions_currency_fkey",
  add constraint "transactions_currency_fkey"
  foreign key ("currency")
  references "sell"."currencies"
  ("value") on update restrict on delete restrict;

alter table "sell"."transactions" drop constraint "transactions_subscription_fkey",
  add constraint "transactions_subscription_fkey"
  foreign key ("subscription")
  references "sell"."subscriptions"
  ("id") on update restrict on delete restrict;

alter table "sell"."srtripe_servers" rename to "stripe_servers";
