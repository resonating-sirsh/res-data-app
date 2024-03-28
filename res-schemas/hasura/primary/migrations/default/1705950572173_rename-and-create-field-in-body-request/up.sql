alter table "meta"."body_requests" rename column "pocket_material_id" to "combo_material_id";
alter table "meta"."body_requests" rename column "size_ranges" to "size_scale_id";
alter table "meta"."body_requests" add column "base_size_code" text;
