alter table "make"."one_pieces_history" drop constraint "one_pieces_history_piece_oid_fkey",
  add constraint "one_pieces_history_piece_oid_fkey"
  foreign key ("piece_oid")
  references "make"."one_pieces"
  ("oid") on update no action on delete no action;
