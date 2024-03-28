

alter table "meta"."body_pieces" drop constraint "body_pieces_body_id_fkey",
  add constraint "body_pieces_body_id_fkey"
  foreign key ("body_id")
  references "meta"."bodies"
  ("id") on update cascade on delete restrict;

alter table "meta"."pieces" drop constraint "pieces_body_piece_id_fkey",
  add constraint "pieces_body_piece_id_fkey"
  foreign key ("body_piece_id")
  references "meta"."body_pieces"
  ("id") on update cascade on delete restrict;

alter table "meta"."pieces" drop constraint "pieces_body_piece_id_fkey",
  add constraint "pieces_body_piece_id_fkey"
  foreign key ("body_piece_id")
  references "meta"."body_pieces"
  ("id") on update cascade on delete restrict;

alter table "meta"."body_pieces" drop constraint "body_pieces_body_id_fkey",
  add constraint "body_pieces_body_id_fkey"
  foreign key ("body_id")
  references "meta"."bodies"
  ("id") on update cascade on delete cascade;

alter table "meta"."body_pieces" drop constraint "body_pieces_body_id_fkey",
  add constraint "body_pieces_body_id_fkey"
  foreign key ("body_id")
  references "meta"."bodies"
  ("id") on update cascade on delete cascade;
