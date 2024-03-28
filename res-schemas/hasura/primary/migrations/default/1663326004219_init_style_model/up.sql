
create schema IF NOT EXISTS "meta";

CREATE TABLE IF NOT EXISTS "meta"."styles" ("id" uuid NOT NULL, "name" text NOT NULL, "body_code" text NOT NULL, "labels" jsonb NOT NULL, "status" text NOT NULL, "metadata" jsonb NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "modified_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."styles" IS E'this is the style project space for the designer to describe intent - it is non physical';

CREATE TABLE IF NOT EXISTS "meta"."style_sizes" ("id" uuid NOT NULL, "size_code" text NOT NULL, "status" text NOT NULL, "labels" jsonb NOT NULL, "metadata" jsonb NOT NULL, "price" numeric NOT NULL, "cost_at_onboarding" numeric NOT NULL, "style_id" uuid NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") , FOREIGN KEY ("style_id") REFERENCES "meta"."styles"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "meta"."style_sizes" IS E'the instance of a style that has a size and we can make it';

CREATE TABLE IF NOT EXISTS "meta"."pieces" ("id" uuid NOT NULL, "code" text NOT NULL, "versioned_body_code" text NOT NULL, "color_code" text NOT NULL, "artwork_uri" text NOT NULL, "material_code" text NOT NULL, "base_image_uri" text, "geometry" text NOT NULL, "notches" text NOT NULL, "internal_lines" text, "corners" text NOT NULL, "seam_metadata" jsonb NOT NULL, "piece_width" numeric NOT NULL, "piece_height" numeric NOT NULL, "metadata" jsonb NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."pieces" IS E'these are specific pieces that can be made and linked to styles';

CREATE TABLE IF NOT EXISTS "meta"."style_size_pieces" ("id" uuid NOT NULL, "style_size_id" uuid NOT NULL, "piece_id" uuid NOT NULL, "started_at" timestamptz NOT NULL, "ended_at" timestamptz NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("piece_id") REFERENCES "meta"."pieces"("id") ON UPDATE restrict ON DELETE restrict, FOREIGN KEY ("style_size_id") REFERENCES "meta"."style_sizes"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "meta"."style_size_pieces" IS E'links the historic link between a style and pieces';

CREATE TABLE IF NOT EXISTS "meta"."bodies" ("id" uuid NOT NULL, "code" text NOT NULL, "brand_code" text NOT NULL, "version" numeric NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "modified_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") );COMMENT ON TABLE "meta"."bodies" IS E'a versioned body';

alter table "meta"."pieces" alter column "piece_width" drop not null;

alter table "meta"."pieces" alter column "piece_height" drop not null;

alter table "meta"."pieces" alter column "seam_metadata" drop not null;

alter table "meta"."pieces" alter column "corners" drop not null;

alter table "meta"."pieces" alter column "notches" drop not null;

alter table "meta"."pieces" alter column "geometry" drop not null;

alter table "meta"."pieces" add column "size_code" text
 not null;

alter table "meta"."style_size_pieces" alter column "ended_at" drop not null;

ALTER TABLE "meta"."style_size_pieces" ALTER COLUMN "started_at" TYPE time;
alter table "meta"."style_size_pieces" alter column "started_at" set default now();

ALTER TABLE "meta"."style_size_pieces" ALTER COLUMN "ended_at" TYPE timestamp;
