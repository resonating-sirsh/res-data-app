
alter table "meta"."bodies" add column "model_3d_uri" text
 null;

alter table "meta"."bodies" add column "point_cloud_uri" text
 null;

alter table "meta"."styles" add column "model_3d_uri" Text
 null;

alter table "meta"."styles" add column "point_cloud_uri" text
 null;
