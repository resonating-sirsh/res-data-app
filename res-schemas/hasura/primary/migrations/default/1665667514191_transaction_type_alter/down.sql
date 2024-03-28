
alter table "sell"."transactions" drop constraint "transactions_source_fkey";

DELETE FROM "sell"."transaction_sources" WHERE "value" = 'subscription_recharge';

DELETE FROM "sell"."transaction_sources" WHERE "value" = 'order';

DROP TABLE "sell"."transaction_sources";

alter table "sell"."transactions" drop constraint "transactions_type_fkey";
