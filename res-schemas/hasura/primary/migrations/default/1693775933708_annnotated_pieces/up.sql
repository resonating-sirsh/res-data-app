
alter table "meta"."pieces" add column "annotated_image_uri" text
 null;

alter table "meta"."pieces" add column "annotated_image_s3_file_version_id" text
 null;


comment on column "meta"."pieces"."annotated_image_uri" is E'the uri for the image png. convention will be to save the outlines in the catch too under path_parent/outlines/*.parquet';
