
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_order_skus_to_ones" add column "order_size" integer
--  null;

DROP TABLE "infraestructure"."bridge_order_skus_to_ones";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_piece_at_node_consumption" add column "one_number" text
--  null;

alter table "make"."one_piece_at_node_consumption"
  add constraint "one_piece_at_node_consumption_one_piece_id_fkey"
  foreign key ("one_piece_id")
  references "make"."one_pieces"
  ("oid") on update no action on delete no action;

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
  ("oid") on update restrict on delete restrict;
