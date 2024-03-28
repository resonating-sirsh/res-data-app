alter table "make"."roll_inspection" drop constraint "roll_inspection_piece_id_fkey",
  add constraint "roll_inspection_piece_id_fkey"
  foreign key ("piece_id")
  references "make"."piece_instances"
  ("id") on update no action on delete restrict;
