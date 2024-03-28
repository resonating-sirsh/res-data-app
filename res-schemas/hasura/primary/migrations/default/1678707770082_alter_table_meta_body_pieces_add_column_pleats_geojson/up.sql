alter table "meta"."body_pieces" add column "pleats_geojson" jsonb
 not null default '{}'::jsonb;

alter table "meta"."body_pieces" add column "buttons_geojson" jsonb
 not null default '{}'::jsonb;
