alter table "make"."one_pieces_costs"
  add constraint "one_pieces_costs_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("id") on update restrict on delete restrict;
