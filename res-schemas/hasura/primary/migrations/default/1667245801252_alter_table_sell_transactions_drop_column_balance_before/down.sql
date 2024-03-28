comment on column "sell"."transactions"."balance_before" is E'This are subscription balance usages for brands.';
alter table "sell"."transactions" alter column "balance_before" drop not null;
alter table "sell"."transactions" add column "balance_before" int4;
