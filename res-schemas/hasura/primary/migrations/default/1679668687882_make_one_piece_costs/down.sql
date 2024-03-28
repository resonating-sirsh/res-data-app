
alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_fkey";

alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_key";

comment on column "make"."one_pieces_costs"."ink_consumption_array" is E'This table contains costs data per resource for each one_piece.';
alter table "make"."one_pieces_costs" alter column "ink_consumption_array" drop not null;
alter table "make"."one_pieces_costs" add column "ink_consumption_array" _float8;

comment on column "make"."one_pieces_costs"."test_array" is E'This table contains costs data per resource for each one_piece.';
alter table "make"."one_pieces_costs" alter column "test_array" set default '{5}'::numeric[];
alter table "make"."one_pieces_costs" alter column "test_array" drop not null;
alter table "make"."one_pieces_costs" add column "test_array" _numeric;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces_costs" add column "test_array" Numeric[]
--  null default '{5}';

ALTER TABLE "make"."one_pieces_costs" ALTER COLUMN "ink_consumption_array" TYPE ARRAY;

ALTER TABLE "make"."one_pieces_costs" ALTER COLUMN "ink_consumption_array" TYPE ARRAY;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces_costs" add column "ink_consumption_array" Numeric[]
--  null;

alter table "make"."one_pieces_costs"
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update set default on delete set default;

alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_fkey",
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update set null on delete set null;

alter table "make"."one_pieces_costs" drop constraint "one_pieces_costs_id_fkey",
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update restrict on delete restrict;

DROP TABLE "make"."one_pieces_costs";
