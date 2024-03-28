alter table "infraestructure"."bridge_sku_one_counter" rename column "make_assets_checked_at" to "print_assets_cached_at";
ALTER TABLE "infraestructure"."bridge_sku_one_counter" ALTER COLUMN "print_assets_cached_at" TYPE timestamp with time zone;
