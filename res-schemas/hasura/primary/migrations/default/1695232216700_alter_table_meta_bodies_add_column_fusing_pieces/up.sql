alter table "meta"."bodies" add column "fusing_pieces" jsonb
 not null default jsonb_build_array();
