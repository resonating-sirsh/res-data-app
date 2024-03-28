alter table "make"."one_pieces"
  add constraint "one_pieces_one_piece_costs_id_fkey"
  foreign key ("one_piece_costs_id")
  references "make"."one_pieces_costs"
  ("id") on update restrict on delete restrict;
