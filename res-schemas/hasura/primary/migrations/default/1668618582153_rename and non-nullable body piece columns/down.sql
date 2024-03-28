
alter table "meta"."body_pieces" alter column "outer_geojson" drop not null;

alter table "meta"."body_pieces" alter column "inner_geojson" drop not null;

alter table "meta"."body_pieces" alter column "outer_edges_geojson" drop not null;

alter table "meta"."body_pieces" alter column "inner_edges_geojson" drop not null;

alter table "meta"."body_pieces" alter column "outer_corners_geojson" drop not null;

alter table "meta"."body_pieces" alter column "outer_notches_geojson" drop not null;

alter table "meta"."body_pieces" alter column "seam_guides_geojson" drop not null;

alter table "meta"."body_pieces" alter column "internal_lines_geojson" drop not null;

alter table "meta"."body_pieces" alter column "metadata" set not null;

alter table "meta"."body_pieces" alter column "seam_guides_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_notches_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_corners_geojson" set not null;

alter table "meta"."body_pieces" alter column "inner_edges_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_edges_geojson" set not null;

alter table "meta"."body_pieces" alter column "inner_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_geojson" set not null;

alter table "meta"."body_pieces" alter column "internal_lines_geojson" set not null;

alter table "meta"."body_pieces" alter column "metadata" drop not null;
ALTER TABLE "meta"."body_pieces" ALTER COLUMN "metadata" drop default;

alter table "meta"."body_pieces" alter column "size_code" drop not null;

alter table "meta"."body_pieces" alter column "internal_lines_geojson" drop not null;
ALTER TABLE "meta"."body_pieces" ALTER COLUMN "internal_lines_geojson" drop default;

alter table "meta"."body_pieces" alter column "seam_guides_geojson" drop not null;
ALTER TABLE "meta"."body_pieces" ALTER COLUMN "seam_guides_geojson" drop default;

ALTER TABLE "meta"."body_pieces" ALTER COLUMN "outer_notches_geojson" drop default;

ALTER TABLE "meta"."body_pieces" ALTER COLUMN "outer_corners_geojson" drop default;

ALTER TABLE "meta"."body_pieces" ALTER COLUMN "inner_edges_geojson" drop default;

ALTER TABLE "meta"."body_pieces" ALTER COLUMN "outer_edges_geojson" drop default;

ALTER TABLE "meta"."body_pieces" ALTER COLUMN "inner_geojson" drop default;

ALTER TABLE "meta"."body_pieces" ALTER COLUMN "outer_geojson" drop default;

alter table "meta"."body_pieces" rename column "piece_key" to "vs_key";

alter table "meta"."bodies" rename column "body_code" to "code";
