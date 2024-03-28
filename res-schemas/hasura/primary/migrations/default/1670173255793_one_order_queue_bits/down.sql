
alter table "make"."one_orders" drop constraint "one_orders_style_size_id_fkey";

comment on column "make"."one_orders"."style_group" is E'orders for ones that bridge meta ones and allow for piece tracking';
alter table "make"."one_orders" alter column "style_group" drop not null;
alter table "make"."one_orders" add column "style_group" text;

comment on column "make"."one_orders"."style_rank" is E'orders for ones that bridge meta ones and allow for piece tracking';
alter table "make"."one_orders" alter column "style_rank" drop not null;
alter table "make"."one_orders" add column "style_rank" int4;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "style_size_id" uuid
--  null;

comment on column "make"."one_orders"."one_code_id" is E'orders for ones that bridge meta ones and allow for piece tracking';
alter table "make"."one_orders" alter column "one_code_id" drop not null;
alter table "make"."one_orders" add column "one_code_id" uuid;


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "style_rank" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "style_group" text
--  null;

alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey";

comment on column "make"."one_pieces"."meta_piece_id_remove" is E'the individual attempts to make pieces for a one';
alter table "make"."one_pieces" alter column "meta_piece_id_remove" drop not null;
alter table "make"."one_pieces" add column "meta_piece_id_remove" uuid;

alter table "make"."one_pieces"
  add constraint "one_pieces_meta_piece_id_fkey2"
  foreign key ("meta_piece_id_remove")
  references "meta"."pieces"
  ("id") on update restrict on delete restrict;

alter table "make"."one_pieces"
  add constraint "one_pieces_meta_piece_id_fkey"
  foreign key ("meta_piece_id")
  references "meta"."style_size_pieces"
  ("id") on update restrict on delete restrict;

alter table "make"."one_pieces" alter column "meta_piece_id_remove" set not null;

alter table "make"."one_pieces" rename column "meta_piece_id" to "meta_piece";

alter table "make"."one_pieces" rename column "meta_piece_id_remove" to "meta_piece_id";

alter table "make"."one_pieces" rename column "meta_piece" to "style_size_meta_piece_id";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "code" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- --SELECT * FROM meta.styles where name = 'SS Riley Dress - Viridian Olive in Cupro Twill'
--
-- --SELECT * from meta.style_sizes where status <>  'PENDING_PIECES'
--
-- -- UPDATE meta.body_pieces SET internal_lines_geojson='{}'::jsonb WHERE internal_lines_geojson is null
-- --alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey";
--
-- DROP TABLE make.one_codes CASCADE;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "one_code_uri" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "make_instance" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "customization" jsonb
--  null default jsonb_build_object();

comment on column "make"."one_orders"."style_group" is E'orders for ones that bridge meta ones and allow for piece tracking';
alter table "make"."one_orders" alter column "style_group" drop not null;
alter table "make"."one_orders" add column "style_group" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "style_group" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_codes" add column "size_code" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_codes" add column "make_instance" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_codes" add column "customization" jsonb
--  null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "flags" jsonb
--  null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "sku" text
--  null;

alter table "make"."one_codes" rename column "style_instance" to "garment_instance";

alter table "make"."one_codes" rename column "style_group" to "unique_garment_key";

comment on column "meta"."styles"."rank" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."styles" add column "rank" int
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "status" text
--  null;

alter table "make"."one_pieces" drop constraint "one_pieces_node_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "node_id" uuid
--  null;

comment on column "make"."one_pieces"."node" is E'the individual attempts to make pieces for a one';
alter table "make"."one_pieces" alter column "node" drop not null;
alter table "make"."one_pieces" add column "node" text;

DROP TABLE "flow"."nodes";

drop schema "flow" cascade;

alter table "make"."one_orders" drop constraint "one_orders_one_code_id_fkey";

comment on column "make"."one_codes"."metadata" is NULL;

DROP TABLE "make"."one_codes";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "one_code_id" uuid
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "node" text
--  null;


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "uri" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."airtable_updates" add column "trace" text
--  null;


alter table "make"."one_orders" alter column "order_item_id" drop not null;

alter table "make"."one_orders" drop constraint "one_orders_order_item_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_orders" add column "order_item_id" uuid
--  null;

comment on column "infraestructure"."airtable_updates"."processed_at" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."airtable_updates" add column "processed_at" timestamptz
--  null;

alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey2";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "meta_piece_id" uuid
--  not null;

alter table "make"."one_pieces" rename column "style_size_meta_piece_id" to "meta_piece_id";
alter table "make"."one_pieces" alter column "meta_piece_id" set not null;

alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey";

comment on column "make"."one_pieces"."make_instance" is NULL;

comment on column "make"."one_pieces"."validation_flags" is E'when we inspect the piece, we can save the last inspection date';

comment on column "make"."one_pieces"."validation_flags" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "metadata" jsonb
--  null default jsonb_build_object();

DROP TABLE "make"."one_pieces";

DROP TABLE "make"."one_orders";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."products" add column "metadata" jsonb
--  null default jsonb_build_array();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."airtable_updates" add column "deleted_at" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."airtable_updates" add column "compressed_payload" text
--  null;

comment on column "infraestructure"."airtable_updates"."key" is NULL;

comment on column "infraestructure"."airtable_updates"."rid" is NULL;

comment on column "infraestructure"."airtable_updates"."hid" is NULL;

comment on column "infraestructure"."airtable_updates"."name" is NULL;

DROP TABLE "infraestructure"."airtable_updates";
