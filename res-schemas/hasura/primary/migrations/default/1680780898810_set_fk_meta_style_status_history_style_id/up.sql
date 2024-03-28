alter table "meta"."style_status_history"
  add constraint "style_status_history_style_id_fkey"
  foreign key ("style_id")
  references "meta"."styles"
  ("id") on update restrict on delete restrict;
