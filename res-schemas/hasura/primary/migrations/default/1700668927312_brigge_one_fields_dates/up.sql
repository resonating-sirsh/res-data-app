
alter table "infraestructure"."bridge_sku_one_counter" add column "all_assets_printed_at" timestamptz
 null;

alter table "infraestructure"."bridge_sku_one_counter" add column "pieces_currently_healing" jsonb
 null default jsonb_build_array();

alter table "infraestructure"."bridge_sku_one_counter" add column "all_assets_cut_at" timestamptz
 null;

alter table "infraestructure"."bridge_sku_one_counter" add column "all_assets_completed_at" timestamptz
 null;

alter table "infraestructure"."bridge_sku_one_counter" add column "exited_sew_at" timestamptz
 null;

alter table "infraestructure"."bridge_sku_one_counter" add column "fulfilled_at" timestamptz
 null;
