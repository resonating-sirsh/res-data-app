alter table "make"."one_pieces" add column "defects" jsonb
 null default jsonb_build_array();
