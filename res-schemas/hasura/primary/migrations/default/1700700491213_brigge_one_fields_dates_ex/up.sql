
alter table "infraestructure"."bridge_sku_one_counter" add column "make_node" text
 null;

alter table "infraestructure"."bridge_sku_one_counter" add column "assembly_node" text
 null;

alter table "infraestructure"."bridge_sku_one_counter" add column "contacts_failing" jsonb
 null default jsonb_build_array();

alter table "infraestructure"."bridge_sku_one_counter" rename column "all_assets_cut_at" to "exited_cut_at";

alter table "infraestructure"."bridge_sku_one_counter" rename column "all_assets_completed_at" to "exited_completion_at";

alter table "infraestructure"."bridge_sku_one_counter" rename column "all_assets_printed_at" to "exited_print_at";
