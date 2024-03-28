alter table "sell"."orders"
  add constraint "orders_ecommerce_source_fkey"
  foreign key ("ecommerce_source")
  references "sell"."ecommerce_sources"
  ("source") on update cascade on delete cascade;
