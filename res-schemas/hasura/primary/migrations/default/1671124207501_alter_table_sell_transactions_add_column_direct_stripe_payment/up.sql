alter table "sell"."transactions" add column "direct_stripe_payment" boolean
 not null default 'False';
