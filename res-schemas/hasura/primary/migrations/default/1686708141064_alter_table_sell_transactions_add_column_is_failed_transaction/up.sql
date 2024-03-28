alter table "sell"."transactions" add column "is_failed_transaction" boolean
 not null default 'False';
