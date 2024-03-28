
ALTER TABLE "meta"."body_pieces_with_material" ALTER COLUMN "sewing_time" TYPE integer;

comment on column "meta"."body_pieces_with_material"."id" is E'this is a hash of the body code, version,size_code and piece map';

comment on column "meta"."body_pieces_with_material"."id" is E'this is a hash of the body code, version and piece map';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_pieces_with_material" add column "size_code" text
--  not null;

comment on column "meta"."body_pieces_with_material"."id" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_pieces_with_material" add column "number_of_materials" integer
--  not null;

comment on column "meta"."body_pieces_with_material"."material_compensation_y" is E'for a particular set of pieces and a material combination, compute some things';
alter table "meta"."body_pieces_with_material" alter column "material_compensation_y" drop not null;
alter table "meta"."body_pieces_with_material" add column "material_compensation_y" numeric;

comment on column "meta"."body_pieces_with_material"."material_compensation_x" is E'for a particular set of pieces and a material combination, compute some things';
alter table "meta"."body_pieces_with_material" alter column "material_compensation_x" drop not null;
alter table "meta"."body_pieces_with_material" add column "material_compensation_x" numeric;

comment on column "meta"."body_pieces_with_material"."pieces_area_raw" is E'for a particular set of pieces and a material combination, compute some things';
alter table "meta"."body_pieces_with_material" alter column "pieces_area_raw" drop not null;
alter table "meta"."body_pieces_with_material" add column "pieces_area_raw" numeric;

comment on column "meta"."body_pieces_with_material"."material_offset_buffer_inches" is E'for a particular set of pieces and a material combination, compute some things';
alter table "meta"."body_pieces_with_material" alter column "material_offset_buffer_inches" drop not null;
alter table "meta"."body_pieces_with_material" add column "material_offset_buffer_inches" numeric;

comment on column "meta"."body_pieces_with_material"."material_code" is E'for a particular set of pieces and a material combination, compute some things';
alter table "meta"."body_pieces_with_material" alter column "material_code" drop not null;
alter table "meta"."body_pieces_with_material" add column "material_code" text;

alter table "meta"."body_pieces_with_material" rename column "material_offset_buffer_inches" to "offset_buffer_inches";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_pieces_with_material" add column "pieces_area_raw" numeric
--  not null;

DROP TABLE "meta"."body_pieces_with_material";
