comment on column "meta"."styles"."cover_image" is E'this is the style project space for the designer to describe intent - it is non physical';
alter table "meta"."styles" alter column "cover_image" drop not null;
alter table "meta"."styles" add column "cover_image" text;
