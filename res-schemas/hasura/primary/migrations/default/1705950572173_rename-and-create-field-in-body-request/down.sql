alter table "meta"."body_requests" rename column "combo_material_code" to "pocket_material_id";
alter table "meta"."body_requests" rename column "size_scale_id" to "size_ranges";
alter table "meta"."body_requests" drop column "base_size_code";
