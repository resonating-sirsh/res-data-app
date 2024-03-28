
CREATE TABLE "sell"."transaction_statuses" ("value" text NOT NULL, "comment" text, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."transaction_statuses" IS E'Enum for payment status. ';

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'succeeded', null);

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'processing', null);

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'requires_payment_method', null);

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'requires_confirmation', null);

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'requires_action', null);

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'canceled', null);

INSERT INTO "sell"."transaction_statuses"("value", "comment") VALUES (E'requires_capture', null);

alter table "sell"."transactions"
  add constraint "transactions_transaction_status_fkey"
  foreign key ("transaction_status")
  references "sell"."transaction_statuses"
  ("value") on update restrict on delete restrict;
