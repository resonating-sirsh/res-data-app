
alter table "meta"."bodies" rename column "code" to "body_code";
alter table "meta"."body_pieces" rename column "vs_key" to "piece_key";

alter table "meta"."body_pieces" alter column "metadata" set default '{}';

alter table "meta"."body_pieces" alter column "size_code" set not null;

alter table "meta"."body_pieces" alter column "seam_guides_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "seam_guides_geojson" set not null;

alter table "meta"."body_pieces" alter column "internal_lines_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "internal_lines_geojson" set not null;


alter table "meta"."body_pieces" alter column "outer_notches_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "outer_notches_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_corners_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "outer_corners_geojson" set not null;

alter table "meta"."body_pieces" alter column "inner_edges_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "inner_edges_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_edges_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "outer_edges_geojson" set not null;

alter table "meta"."body_pieces" alter column "inner_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "inner_geojson" set not null;

alter table "meta"."body_pieces" alter column "outer_geojson" set default '{}';
alter table "meta"."body_pieces" alter column "outer_geojson" set not null;
