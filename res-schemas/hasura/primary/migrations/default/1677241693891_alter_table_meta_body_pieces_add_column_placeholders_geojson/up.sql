alter table "meta"."body_pieces" add column "placeholders_geojson" jsonb
 not null default '{}'::jsonb;
