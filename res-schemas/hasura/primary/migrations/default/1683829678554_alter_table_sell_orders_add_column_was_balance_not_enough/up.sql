alter table "sell"."orders" add column "was_balance_not_enough" boolean
 not null default 'False';
