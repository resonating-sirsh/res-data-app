
alter table "sell"."order_line_items" add column "status" text
 null;

alter table "sell"."order_line_items" add column "cost_at_order" numeric
 null;

alter table "sell"."orders" add column "sales_channel" text
 null;

alter table "sell"."orders" add column "order_channel" text
 null;

alter table "sell"."order_line_items" add column "customization" jsonb
 null;

alter table "sell"."order_line_items" add column "revenue_share_code" text
 null;

alter table "sell"."orders" add column "email" text
 null;

alter table "sell"."order_line_items" drop column "cost_at_order" cascade;

CREATE TABLE "sell"."order_fulfillment" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "order_item_id" uuid NOT NULL, "make_order_id" uuid NOT NULL, "started_at" timestamptz NOT NULL, "ended_at" timestamptz, PRIMARY KEY ("id") );COMMENT ON TABLE "sell"."order_fulfillment" IS E'a softer link between orders and make orders';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "sell"."order_fulfillment" rename to "order_fulfillments";

alter table "sell"."order_fulfillments" rename to "order_item_fulfillments";

comment on table "sell"."order_item_fulfillments" is E'a softer link between orders and make orders - this is per single ONE inside an order item which has quantity';

alter table "sell"."order_item_fulfillments" add column "status" text
 null;

alter table "sell"."order_item_fulfillments" drop column "status" cascade;

alter table "sell"."order_item_fulfillments" add column "status" text
 null;

alter table "sell"."order_item_fulfillments" add column "order_item_ordinal" Numeric
 null;

comment on column "sell"."order_item_fulfillments"."status" is E'added to explain why the fulfillment is open or closed. for example it could be shipped or cancelled by customer';

comment on column "sell"."order_item_fulfillments"."order_item_ordinal" is E'added so that we can uniquely link to one item in an order if we want to. should ship in batch but statuses will diverge';

alter table "sell"."orders" add column "tags" jsonb
 null;

alter table "sell"."orders" add column "item_quantity_hash" text
 null;

alter table "meta"."bodies" add column "metdata" jsonb
 null;

alter table "meta"."bodies" rename column "metdata" to "metadata";

alter table "meta"."pieces" alter column "size_code" drop not null;

alter table "meta"."pieces" drop column "code" cascade;

alter table "meta"."pieces" drop column "versioned_body_code" cascade;

alter table "meta"."pieces" drop column "geometry" cascade;

alter table "meta"."pieces" drop column "corners" cascade;

alter table "meta"."pieces" drop column "internal_lines" cascade;

alter table "meta"."pieces" drop column "seam_metadata" cascade;

alter table "meta"."pieces" drop column "piece_width" cascade;

alter table "meta"."pieces" drop column "piece_height" cascade;

alter table "meta"."pieces" drop column "size_code" cascade;

alter table "meta"."pieces" add column "body_piece_id" uuid
 null;

alter table "meta"."pieces"
  add constraint "pieces_body_piece_id_fkey"
  foreign key ("body_piece_id")
  references "meta"."body_pieces"
  ("id") on update restrict on delete restrict;

alter table "meta"."style_sizes" alter column "price" drop not null;

alter table "meta"."style_sizes" alter column "cost_at_onboarding" drop not null;

alter table "sell"."products" add column "name" text
 null;

alter table "sell"."order_line_items" add column "metadata" jsonb
 null;

alter table "sell"."orders" add column "number" text
 null;

alter table "sell"."orders" rename column "number" to "name";

alter table "sell"."order_line_items" add column "name" text
 null;

alter table "sell"."orders" add column "status" text
 null;

alter table "sell"."order_line_items" add column "product_id" uuid
 null;

alter table "sell"."orders" alter column "source_metadata" drop not null;

alter table "sell"."order_line_items"
  add constraint "order_line_items_product_id_fkey"
  foreign key ("product_id")
  references "sell"."products"
  ("id") on update restrict on delete restrict;
