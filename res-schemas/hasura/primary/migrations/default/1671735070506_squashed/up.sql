

CREATE TABLE "sell"."transaction_detail_type" ("value" text NOT NULL, "comment" text, PRIMARY KEY ("value") , UNIQUE ("value"));

INSERT INTO "sell"."transaction_detail_type"("value", "comment") VALUES (E'item', E'This is an item for resonance.');

INSERT INTO "sell"."transaction_detail_type"("value", "comment") VALUES (E'shipping', E'This is shipping cost for resonance.');

alter table "sell"."transaction_details" add column "type" Text
 null;

alter table "sell"."transaction_details"
  add constraint "transaction_details_type_fkey"
  foreign key ("type")
  references "sell"."transaction_detail_type"
  ("value") on update restrict on delete restrict;

alter table "sell"."transaction_details" alter column "type" set not null;
