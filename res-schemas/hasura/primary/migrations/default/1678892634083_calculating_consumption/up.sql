
alter table "make"."printfile" add column "ink_consumption_ml" jsonb
 null default jsonb_build_object();

alter table "make"."printfile" add column "material_consumption_mm" jsonb
 null default jsonb_build_object();
