
CREATE TABLE "sell"."srtripe_server" ("value" text NOT NULL, "comment" text NOT NULL, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."srtripe_server" IS E'This are the types of servers there are on Stripe.';

INSERT INTO "sell"."srtripe_server"("value", "comment") VALUES (E'development', E'Dev test server');

INSERT INTO "sell"."srtripe_server"("value", "comment") VALUES (E'production', E'Live stripe server');

alter table "sell"."srtripe_server" rename to "srtripe_servers";

CREATE TABLE "sell"."stripe_accounts" ("id" serial NOT NULL, "stripe_customer_id" text NOT NULL, "brand_id" text NOT NULL, "brand_code" text NOT NULL, "name" text, PRIMARY KEY ("id") , UNIQUE ("stripe_customer_id"), UNIQUE ("brand_id"), UNIQUE ("brand_code"), UNIQUE ("id"));COMMENT ON TABLE "sell"."stripe_accounts" IS E'This are Stripe Customers accounsts';

CREATE TABLE "sell"."subscriptions" ("name" text, "subscription_id" text NOT NULL, "id" serial NOT NULL, "deleted_status" text, "account" integer NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "last_payment_at" timestamptz NOT NULL, "next_invoice" date NOT NULL, "start_date" timestamptz NOT NULL, "end_date" timestamptz NOT NULL, "collection_method" text NOT NULL, "payment_history" text NOT NULL, "balance" numeric NOT NULL, "active_status" text NOT NULL, PRIMARY KEY ("account") , FOREIGN KEY ("account") REFERENCES "sell"."stripe_accounts"("id") ON UPDATE restrict ON DELETE restrict, UNIQUE ("subscription_id"), UNIQUE ("id"), UNIQUE ("account"));COMMENT ON TABLE "sell"."subscriptions" IS E'This are Subscription instance for brands on Stripe/Monthly payment and Balance state.';
CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_subscriptions_updated_at"
BEFORE UPDATE ON "sell"."subscriptions"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_subscriptions_updated_at" ON "sell"."subscriptions" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

CREATE TABLE "sell"."subscription_status" ("value" text NOT NULL, "comment" text, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."subscription_status" IS E'This are the posible status an account subscription holds.';

INSERT INTO "sell"."subscription_status"("value", "comment") VALUES (E'active', E'This is active state');

INSERT INTO "sell"."subscription_status"("value", "comment") VALUES (E'inactive', E'This is not active state');

alter table "sell"."subscriptions"
  add constraint "subscriptions_active_status_fkey"
  foreign key ("active_status")
  references "sell"."subscription_status"
  ("value") on update set default on delete set default;

CREATE TABLE "sell"."collection_method" ("value" text NOT NULL, "commment" text, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."collection_method" IS E'This are the posible collections methods from subscriptions';

INSERT INTO "sell"."collection_method"("value", "commment") VALUES (E'charge_automatically', E'This is the default');

INSERT INTO "sell"."collection_method"("value", "commment") VALUES (E'send_invoice', E'Send the invoice method.');

alter table "sell"."collection_method" rename to "collection_methods";

alter table "sell"."subscriptions"
  add constraint "subscriptions_collection_method_fkey"
  foreign key ("collection_method")
  references "sell"."collection_methods"
  ("value") on update set default on delete set default;

alter table "sell"."subscriptions" drop column "active_status" cascade;

CREATE TABLE "sell"."transactions" ("id" serial NOT NULL, "amount" numeric NOT NULL, "subscription" integer NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "balance_before" numeric NOT NULL, "balance_after" numeric NOT NULL, "type" text NOT NULL, "source" Text NOT NULL, "reference_id" text NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("subscription") REFERENCES "sell"."subscriptions"("id") ON UPDATE set default ON DELETE set default, UNIQUE ("id"));COMMENT ON TABLE "sell"."transactions" IS E'This are subscription balance usages for brands.';
CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_transactions_updated_at"
BEFORE UPDATE ON "sell"."transactions"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_transactions_updated_at" ON "sell"."transactions" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

CREATE TABLE "sell"."transaction_types" ("value" text NOT NULL, "comment" serial, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."transaction_types" IS E'This represents ';

alter table "sell"."transaction_types" drop column "comment" cascade;

alter table "sell"."transaction_types" add column "comment" text
 null;

INSERT INTO "sell"."transaction_types"("value", "comment") VALUES (E'debit', E'Represents a positive transaction');

INSERT INTO "sell"."transaction_types"("value", "comment") VALUES (E'credit', E'Represents a negative transaction');
