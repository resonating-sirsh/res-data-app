
alter table "meta"."body_request_asset_nodes" rename to "body_request_asset_annotations";

alter table "meta"."body_request_asset_annotations" add column "artwork_uri" text
 null;

alter table "meta"."body_request_asset_annotations" rename column "type" to "annotation_type";

alter table "meta"."body_request_asset_annotations" add column "main_label_uri" text
 null;

alter table "meta"."body_request_asset_annotations" add column "size_label_uri" text
 null;

alter table "meta"."body_request_asset_annotations" add column "label_style_id" uuid
 null;

CREATE TABLE "meta"."label_styles" ("id" uuid NOT NULL, "name" text NOT NULL, "thumbnail_uri" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));

alter table "meta"."body_request_assets" drop column "metadata" cascade;
