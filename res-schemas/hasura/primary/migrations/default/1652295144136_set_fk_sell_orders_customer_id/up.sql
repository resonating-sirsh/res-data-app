alter table "sell"."orders"
  add constraint "orders_customer_id_fkey"
  foreign key ("customer_id")
  references "sell"."customers"
  ("id") on update cascade on delete cascade;
