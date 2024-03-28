alter table "make"."one_orders" drop constraint "one_orders_order_item_id_fkey",
  add constraint "one_orders_order_item_id_fkey"
  foreign key ("order_item_id")
  references "sell"."order_line_items"
  ("id") on update cascade on delete cascade;
