
alter table "make"."one_piece_healing_requests" add column "inspected_at" timestamptz
 null;

alter table "make"."one_piece_healing_requests" add column "print_asset_piece_id" text
 null;

alter table "make"."one_piece_healing_requests" drop column "inspected_at" cascade;
