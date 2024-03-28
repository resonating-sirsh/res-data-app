
alter table "make"."one_pieces" add column if not exists "one_piece_costs_id" uuid
 null;

comment on column "make"."one_pieces"."one_piece_costs_id" is E'FK for costs assocaited with a piece';
