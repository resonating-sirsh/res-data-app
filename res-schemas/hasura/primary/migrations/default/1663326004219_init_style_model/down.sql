
ALTER TABLE "meta"."style_size_pieces" ALTER COLUMN "ended_at" TYPE timestamp with time zone;

ALTER TABLE "meta"."style_size_pieces" ALTER COLUMN "started_at" drop default;
ALTER TABLE "meta"."style_size_pieces" ALTER COLUMN "started_at" TYPE timestamp with time zone;

alter table "meta"."style_size_pieces" alter column "ended_at" set not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "size_code" text
--  not null;

alter table "meta"."pieces" alter column "geometry" set not null;

alter table "meta"."pieces" alter column "notches" set not null;

alter table "meta"."pieces" alter column "corners" set not null;

alter table "meta"."pieces" alter column "seam_metadata" set not null;

alter table "meta"."pieces" alter column "piece_height" set not null;

alter table "meta"."pieces" alter column "piece_width" set not null;

DROP TABLE "meta"."bodies" cascade;

DROP TABLE "meta"."style_size_pieces" cascade;

DROP TABLE "meta"."pieces" cascade;

DROP TABLE "meta"."style_sizes" cascade;

DROP TABLE "meta"."styles" cascade;
