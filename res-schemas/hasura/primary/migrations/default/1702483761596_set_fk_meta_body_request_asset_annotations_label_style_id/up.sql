alter table "meta"."body_request_asset_annotations"
  add constraint "body_request_asset_annotations_label_style_id_fkey"
  foreign key ("label_style_id")
  references "meta"."label_styles"
  ("id") on update cascade on delete cascade;
