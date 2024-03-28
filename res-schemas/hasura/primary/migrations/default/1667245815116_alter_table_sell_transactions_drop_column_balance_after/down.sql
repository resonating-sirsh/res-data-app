comment on column "sell"."transactions"."balance_after" is E'This are subscription balance usages for brands.';
alter table "sell"."transactions" alter column "balance_after" drop not null;
alter table "sell"."transactions" add column "balance_after" int4;
