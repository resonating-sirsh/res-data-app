
alter table "sell"."transactions"
  add constraint "transactions_type_fkey"
  foreign key ("type")
  references "sell"."transaction_types"
  ("value") on update set default on delete set default;

CREATE TABLE "sell"."transaction_sources" ("value" text NOT NULL, "comment" text, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."transaction_sources" IS E'This represents source of information for the transaction';

INSERT INTO "sell"."transaction_sources"("value", "comment") VALUES (E'order', E'This is a make Order transaction');

INSERT INTO "sell"."transaction_sources"("value", "comment") VALUES (E'subscription_recharge', E'This is at the start of the month for subscription and positive balance.');

alter table "sell"."transactions"
  add constraint "transactions_source_fkey"
  foreign key ("source")
  references "sell"."transaction_sources"
  ("value") on update set default on delete set default;
