
alter table "sell"."transaction_details" drop constraint "transaction_details_type_fkey";

alter table "sell"."transaction_details" drop column "type";

DELETE FROM "sell"."transaction_detail_type" WHERE "value" = 'shipping';

DELETE FROM "sell"."transaction_detail_type" WHERE "value" = 'item';

DROP TABLE "sell"."transaction_detail_type";





