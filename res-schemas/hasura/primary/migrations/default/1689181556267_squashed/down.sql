
alter table "sell"."transactions" drop constraint "transactions_transaction_status_fkey";

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'requires_capture';

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'canceled';

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'requires_action';

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'requires_confirmation';

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'requires_payment_method';

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'processing';

DELETE FROM "sell"."transaction_statuses" WHERE "value" = 'succeeded';

DROP TABLE "sell"."transaction_statuses";
