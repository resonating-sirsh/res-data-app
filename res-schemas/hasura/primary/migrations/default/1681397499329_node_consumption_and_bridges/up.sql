
alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_one_piece_id_fkey",
  add constraint "one_piece_at_node_consumption_one_piece_id_fkey"
  foreign key ("one_piece_id")
  references "make"."one_pieces"
  ("oid") on update no action on delete restrict;

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_one_piece_id_fkey",
  add constraint "one_piece_at_node_consumption_one_piece_id_fkey"
  foreign key ("one_piece_id")
  references "make"."one_pieces"
  ("oid") on update no action on delete restrict;

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_one_piece_id_fkey",
  add constraint "one_piece_at_node_consumption_one_piece_id_fkey"
  foreign key ("one_piece_id")
  references "make"."one_pieces"
  ("oid") on update no action on delete no action;

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_one_piece_id_fkey";

alter table "make"."one_piece_at_node_consumption" add column "one_number" text
 null;

CREATE TABLE "infraestructure"."bridge_order_skus_to_ones" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "order_key" text NOT NULL, "sku_map" jsonb NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "infraestructure"."bridge_order_skus_to_ones" IS E'a mapping of what skus have been assigned to one numbers in an order';
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
CREATE TRIGGER "set_infraestructure_bridge_order_skus_to_ones_updated_at"
BEFORE UPDATE ON "infraestructure"."bridge_order_skus_to_ones"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_bridge_order_skus_to_ones_updated_at" ON "infraestructure"."bridge_order_skus_to_ones" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "infraestructure"."bridge_order_skus_to_ones" add column "order_size" integer
 null;
