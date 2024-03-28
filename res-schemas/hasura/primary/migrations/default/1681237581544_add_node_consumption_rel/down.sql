
alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_one_piece_id_fkey",
  add constraint "one_piece_at_node_consumption_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("oid") on update restrict on delete restrict;

alter table "make"."one_piece_at_node_consumption" alter column "one_piece_id" drop not null;

alter table "make"."one_piece_at_node_consumption" alter column "one_piece_id" set not null;

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_node_id_fkey";

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_id_fkey";

alter table "make"."one_piece_at_node_consumption" alter column "ids" set default 0;
alter table "make"."one_piece_at_node_consumption" alter column "ids" drop not null;
alter table "make"."one_piece_at_node_consumption" add column "ids" int4;

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_pkey";
alter table "make"."one_piece_at_node_consumption"
    add constraint "one_piece_node_consumption_pkey"
    primary key ("ids");

alter table "make"."one_piece_at_node_consumption" alter column "ids" set default nextval('make.one_piece_node_consumption_id_seq'::regclass);

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_piece_at_node_consumption" add column "id" uuid
--  null;

alter table "make"."one_piece_at_node_consumption" rename column "ids" to "id";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_piece_at_node_consumption" add column "category" text
--  null;

alter table "make"."one_piece_at_node_consumption" rename to "one_piece_node_consumption";

DROP TABLE "make"."one_piece_node_consumption";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "deleted_at" timestamptz
--  null;
