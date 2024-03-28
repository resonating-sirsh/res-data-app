
ALTER TABLE "infraestructure"."airtable_cache_metadata_tables_in_base" ALTER COLUMN "id" drop default;

alter table "infraestructure"."airtable_cache_metadata_tables_in_base" rename to "airtable_cache_tables_in_base";

alter table "infraestructure"."airtable_cache_metadata_table_fields" rename to "airtable_cache_table_fields";

ALTER TABLE "infraestructure"."airtable_cache_metadata_bases" ALTER COLUMN "id" drop default;

DROP TABLE "infraestructure"."airtable_cache_table_fields";

DROP TABLE "infraestructure"."airtable_cache_tables_in_base";

DROP TABLE "infraestructure"."airtable_cache_metadata_bases";
