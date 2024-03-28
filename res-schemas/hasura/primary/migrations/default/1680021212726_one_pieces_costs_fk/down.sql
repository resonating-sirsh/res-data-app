
comment on column "make"."one_pieces"."one_pieces_costs" is E'the individual attempts to make pieces for a one';
alter table "make"."one_pieces" alter column "one_pieces_costs" drop not null;
alter table "make"."one_pieces" add column "one_pieces_costs" uuid;

comment on column "make"."one_pieces"."one_piece_costs_id" is NULL;

comment on column "make"."one_pieces"."one_pieces_costs" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "one_pieces_costs" uuid
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces" add column "one_piece_costs_id" uuid
--  null;
