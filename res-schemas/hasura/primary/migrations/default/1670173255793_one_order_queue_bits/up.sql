



CREATE TABLE "infraestructure"."airtable_updates" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), "name" text NOT NULL, "rid" text, "key" text NOT NULL, "hid" uuid, PRIMARY KEY ("id") );COMMENT ON TABLE "infraestructure"."airtable_updates" IS E'a process to update one platform airtable queues';
CREATE OR REPLACE FUNCTION "infraestructure"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_infraestructure_airtable_updates_updated_at"
BEFORE UPDATE ON "infraestructure"."airtable_updates"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_airtable_updates_updated_at" ON "infraestructure"."airtable_updates" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

comment on column "infraestructure"."airtable_updates"."name" is E'the name of the entity e.g.  something that points to kafka topics and hasura tables';

comment on column "infraestructure"."airtable_updates"."hid" is E'the hasura unique id';

comment on column "infraestructure"."airtable_updates"."rid" is E'the airtable record id';

comment on column "infraestructure"."airtable_updates"."key" is E'the business key value e.g. a body code or one number';

alter table "infraestructure"."airtable_updates" add column "compressed_payload" text
 null;

alter table "infraestructure"."airtable_updates" add column "deleted_at" timestamptz
 null;



CREATE TABLE "make"."one_orders" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "one_number" serial NOT NULL, "one_code" text NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), PRIMARY KEY ("id") );COMMENT ON TABLE "make"."one_orders" IS E'orders for ones that bridge meta ones and allow for piece tracking';
CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_make_one_orders_updated_at"
BEFORE UPDATE ON "make"."one_orders"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_orders_updated_at" ON "make"."one_orders" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "make"."one_pieces" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "meta_piece_id" uuid NOT NULL, "make_instance" numeric NOT NULL DEFAULT 1, "validation_flags" jsonb, "inspected_at" timestamptz, "one_order_id" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("one_order_id") REFERENCES "make"."one_orders"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "make"."one_pieces" IS E'the individual attempts to make pieces for a one';
CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_make_one_pieces_updated_at"
BEFORE UPDATE ON "make"."one_pieces"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_pieces_updated_at" ON "make"."one_pieces" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."one_pieces" add column "metadata" jsonb
 null default jsonb_build_object();

comment on column "make"."one_pieces"."validation_flags" is E'when we inspect the piece, we can save the last inspection date';

comment on column "make"."one_pieces"."validation_flags" is E'when we inspect the piece, we can save the last inspection date along with validation flags';

comment on column "make"."one_pieces"."make_instance" is E'a counter for the number of times we attempt to make this piece with healings';

alter table "make"."one_pieces"
  add constraint "one_pieces_meta_piece_id_fkey"
  foreign key ("meta_piece_id")
  references "meta"."style_size_pieces"
  ("id") on update restrict on delete restrict;

alter table "make"."one_pieces" alter column "meta_piece_id" drop not null;
alter table "make"."one_pieces" rename column "meta_piece_id" to "style_size_meta_piece_id";

alter table "make"."one_pieces" add column "meta_piece_id" uuid
 not null;

alter table "make"."one_pieces"
  add constraint "one_pieces_meta_piece_id_fkey2"
  foreign key ("meta_piece_id")
  references "meta"."pieces"
  ("id") on update restrict on delete restrict;

alter table "infraestructure"."airtable_updates" add column "processed_at" timestamptz
 null;

comment on column "infraestructure"."airtable_updates"."processed_at" is E'when we have sent the given data to airtable then we can say we processed - we generally only save a payload if we fail too';

alter table "make"."one_orders" add column "order_item_id" uuid
 null;

alter table "make"."one_orders"
  add constraint "one_orders_order_item_id_fkey"
  foreign key ("order_item_id")
  references "sell"."order_line_items"
  ("id") on update restrict on delete restrict;

alter table "make"."one_orders" alter column "order_item_id" set not null;

alter table "infraestructure"."airtable_updates" add column "trace" text
 null;

alter table "make"."one_pieces" add column "uri" text
 null;

alter table "make"."one_pieces" add column "node" text
 null;

alter table "make"."one_orders" add column "one_code_id" uuid
 null;

CREATE TABLE "make"."one_codes" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "code" text NOT NULL, "one_number" serial NOT NULL, "uri" text, "unique_garment_key" uuid NOT NULL, "garment_instance" integer NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), PRIMARY KEY ("id") );COMMENT ON TABLE "make"."one_codes" IS E'register one codes';
CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_make_one_codes_updated_at"
BEFORE UPDATE ON "make"."one_codes"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_codes_updated_at" ON "make"."one_codes" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

comment on column "make"."one_codes"."metadata" is E'store sku, brand and customizations etc - things that explain the one code';

alter table "make"."one_orders"
  add constraint "one_orders_one_code_id_fkey"
  foreign key ("one_code_id")
  references "make"."one_codes"
  ("id") on update restrict on delete restrict;

create schema "flow";

CREATE TABLE "flow"."nodes" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, PRIMARY KEY ("id") );
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_nodes_updated_at"
BEFORE UPDATE ON "flow"."nodes"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_nodes_updated_at" ON "flow"."nodes" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."one_pieces" drop column "node" cascade;

alter table "make"."one_pieces" add column "node_id" uuid
 null;

alter table "make"."one_pieces"
  add constraint "one_pieces_node_id_fkey"
  foreign key ("node_id")
  references "flow"."nodes"
  ("id") on update restrict on delete restrict;

alter table "make"."one_orders" add column "status" text
 null;

alter table "meta"."styles" add column "rank" int
 null;

comment on column "meta"."styles"."rank" is E'a style number - over some group e.g. body code, ordered by onboarding date from source';

alter table "make"."one_codes" rename column "unique_garment_key" to "style_group";

alter table "make"."one_codes" rename column "garment_instance" to "style_instance";

alter table "make"."one_orders" add column "sku" text
 null;

alter table "make"."one_orders" add column "flags" jsonb
 null default jsonb_build_object();

alter table "make"."one_codes" add column "customization" jsonb
 null default jsonb_build_object();

alter table "make"."one_codes" add column "make_instance" integer
 null;

alter table "make"."one_codes" add column "size_code" text
 null;

alter table "make"."one_orders" add column "style_group" text
 null;

alter table "make"."one_orders" drop column "style_group" cascade;

alter table "make"."one_orders" add column "customization" jsonb
 null default jsonb_build_object();

alter table "make"."one_orders" add column "make_instance" integer
 null;

alter table "make"."one_orders" add column "one_code_uri" text
 null;

--SELECT * FROM meta.styles where name = 'SS Riley Dress - Viridian Olive in Cupro Twill'

--SELECT * from meta.style_sizes where status <>  'PENDING_PIECES'

-- UPDATE meta.body_pieces SET internal_lines_geojson='{}'::jsonb WHERE internal_lines_geojson is null
--alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey";

DROP TABLE make.one_codes CASCADE;

alter table "make"."one_pieces" add column "code" text
 null;

alter table "make"."one_pieces" rename column "style_size_meta_piece_id" to "meta_piece";

alter table "make"."one_pieces" rename column "meta_piece_id" to "meta_piece_id_remove";

alter table "make"."one_pieces" rename column "meta_piece" to "meta_piece_id";

alter table "make"."one_pieces" alter column "meta_piece_id_remove" drop not null;

alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey";

alter table "make"."one_pieces" drop constraint "one_pieces_meta_piece_id_fkey2";

alter table "make"."one_pieces" drop column "meta_piece_id_remove" cascade;

alter table "make"."one_pieces"
  add constraint "one_pieces_meta_piece_id_fkey"
  foreign key ("meta_piece_id")
  references "meta"."pieces"
  ("id") on update restrict on delete restrict;

alter table "make"."one_orders" add column "style_group" text
 null;

alter table "make"."one_orders" add column "style_rank" integer
 null;

alter table "make"."one_orders" drop column "one_code_id" cascade;

alter table "make"."one_orders" add column "style_size_id" uuid
 null;

alter table "make"."one_orders" drop column "style_rank" cascade;

alter table "make"."one_orders" drop column "style_group" cascade;

alter table "make"."one_orders"
  add constraint "one_orders_style_size_id_fkey"
  foreign key ("style_size_id")
  references "meta"."style_sizes"
  ("id") on update restrict on delete restrict;
