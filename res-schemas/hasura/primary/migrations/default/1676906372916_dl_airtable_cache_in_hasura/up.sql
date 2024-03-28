
CREATE TABLE "infraestructure"."airtable_cache_metadata_bases" ("id" uuid NOT NULL, "last_updated" timestamptz NOT NULL DEFAULT now(), "base_id" text NOT NULL, "permission_level" Text NOT NULL, "clean_name" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));COMMENT ON TABLE "infraestructure"."airtable_cache_metadata_bases" IS E'holds info about what bases airtable has for caching purposes';

CREATE TABLE "infraestructure"."airtable_cache_tables_in_base" ("id" uuid NOT NULL, "last_updated" timestamptz NOT NULL DEFAULT now(), "base_id" text NOT NULL, "table_clean_name" text NOT NULL, "table_id" text NOT NULL, "primary_key_field_id" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));COMMENT ON TABLE "infraestructure"."airtable_cache_tables_in_base" IS E'holds info about what tables airtable has in a given base, for caching purposes';

CREATE TABLE "infraestructure"."airtable_cache_table_fields" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "last_updated" timestamptz NOT NULL DEFAULT now(), "field_id" text NOT NULL, "field_name" text NOT NULL, "field_type_airtable" text NOT NULL, "is_primary_key_field" boolean NOT NULL, "field_desc" text NOT NULL, "table_id" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));COMMENT ON TABLE "infraestructure"."airtable_cache_table_fields" IS E'holds info about fields in a given table, including type etc';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "infraestructure"."airtable_cache_metadata_bases" alter column "id" set default gen_random_uuid();

alter table "infraestructure"."airtable_cache_table_fields" rename to "airtable_cache_metadata_table_fields";

alter table "infraestructure"."airtable_cache_tables_in_base" rename to "airtable_cache_metadata_tables_in_base";

alter table "infraestructure"."airtable_cache_metadata_tables_in_base" alter column "id" set default gen_random_uuid();

ALTER TABLE "infraestructure"."airtable_cache_metadata_bases" ALTER COLUMN "id" drop default;

ALTER TABLE "infraestructure"."airtable_cache_metadata_tables_in_base" ALTER COLUMN "id" drop default;

ALTER TABLE "infraestructure"."airtable_cache_metadata_table_fields" ALTER COLUMN "id" drop default;
