
alter table "sell"."order_line_items" drop constraint "order_line_items_product_id_fkey";

alter table "sell"."orders" alter column "source_metadata" set not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "product_id" uuid
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "status" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "name" text
--  null;

alter table "sell"."orders" rename column "name" to "number";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "number" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "metadata" jsonb
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."products" add column "name" text
--  null;

alter table "meta"."style_sizes" alter column "cost_at_onboarding" set not null;

alter table "meta"."style_sizes" alter column "price" set not null;

alter table "meta"."pieces" drop constraint "pieces_body_piece_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "body_piece_id" uuid
--  null;

comment on column "meta"."pieces"."size_code" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "size_code" drop not null;
alter table "meta"."pieces" add column "size_code" text;

comment on column "meta"."pieces"."piece_height" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "piece_height" drop not null;
alter table "meta"."pieces" add column "piece_height" numeric;

comment on column "meta"."pieces"."piece_width" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "piece_width" drop not null;
alter table "meta"."pieces" add column "piece_width" numeric;

comment on column "meta"."pieces"."seam_metadata" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "seam_metadata" drop not null;
alter table "meta"."pieces" add column "seam_metadata" jsonb;

comment on column "meta"."pieces"."internal_lines" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "internal_lines" drop not null;
alter table "meta"."pieces" add column "internal_lines" text;

comment on column "meta"."pieces"."corners" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "corners" drop not null;
alter table "meta"."pieces" add column "corners" text;

comment on column "meta"."pieces"."geometry" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "geometry" drop not null;
alter table "meta"."pieces" add column "geometry" text;

comment on column "meta"."pieces"."versioned_body_code" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "versioned_body_code" drop not null;
alter table "meta"."pieces" add column "versioned_body_code" text;

comment on column "meta"."pieces"."code" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "code" drop not null;
alter table "meta"."pieces" add column "code" text;

alter table "meta"."pieces" alter column "size_code" set not null;

alter table "meta"."bodies" rename column "metadata" to "metdata";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."bodies" add column "metdata" jsonb
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "item_quantity_hash" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "tags" jsonb
--  null;

comment on column "sell"."order_item_fulfillments"."order_item_ordinal" is NULL;

comment on column "sell"."order_item_fulfillments"."status" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_item_fulfillments" add column "order_item_ordinal" Numeric
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_item_fulfillments" add column "status" text
--  null;

comment on column "sell"."order_item_fulfillments"."status" is E'a softer link between orders and make orders - this is per single ONE inside an order item which has quantity';
alter table "sell"."order_item_fulfillments" alter column "status" drop not null;
alter table "sell"."order_item_fulfillments" add column "status" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_item_fulfillments" add column "status" text
--  null;

comment on table "sell"."order_item_fulfillments" is NULL;

alter table "sell"."order_item_fulfillments" rename to "order_fulfillments";

alter table "sell"."order_fulfillments" rename to "order_fulfillment";

DROP TABLE "sell"."order_fulfillment";

comment on column "sell"."order_line_items"."cost_at_order" is E'Individual Line items from orders';
alter table "sell"."order_line_items" alter column "cost_at_order" drop not null;
alter table "sell"."order_line_items" add column "cost_at_order" numeric;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "email" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "revenue_share_code" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "customization" jsonb
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "order_channel" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "sales_channel" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "cost_at_order" numeric
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "status" text
--  null;
