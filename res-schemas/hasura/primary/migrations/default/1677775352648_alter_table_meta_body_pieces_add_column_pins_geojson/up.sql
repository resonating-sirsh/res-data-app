alter table "meta"."body_pieces" add column "pins_geojson" jsonb
 not null default '{}'::jsonb;
