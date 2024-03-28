
alter table "meta"."brands" add column "api_key" text
 null;

alter table "sell"."brands" add column "api_key" text
 null;

alter table "sell"."orders" add column "sell_brands_pkid" integer
 null;

alter table "sell"."orders"
  add constraint "orders_sell_brands_pkid_fkey"
  foreign key ("sell_brands_pkid")
  references "sell"."brands"
  ("id") on update set null on delete set null;
