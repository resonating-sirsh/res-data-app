alter table "sell"."order_item_fulfillments" drop constraint "order_item_fulfillments_order_item_id_fkey",
  add constraint "order_item_fulfillments_order_item_id_fkey"
  foreign key ("order_item_id")
  references "sell"."order_line_items"
  ("id") on update cascade on delete cascade;
