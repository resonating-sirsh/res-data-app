
alter table "infraestructure"."bridge_sku_one_counter" rename column "exited_print_at" to "all_assets_printed_at";

alter table "infraestructure"."bridge_sku_one_counter" rename column "exited_completion_at" to "all_assets_completed_at";

alter table "infraestructure"."bridge_sku_one_counter" rename column "exited_cut_at" to "all_assets_cut_at";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "contacts_failing" jsonb
--  null default jsonb_build_array();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "assembly_node" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."bridge_sku_one_counter" add column "make_node" text
--  null;
