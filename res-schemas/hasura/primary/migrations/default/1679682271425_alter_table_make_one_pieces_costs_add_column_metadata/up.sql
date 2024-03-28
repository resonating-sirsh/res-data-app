alter table "make"."one_pieces_costs" add column "metadata" jsonb
 null default jsonb_build_object();
