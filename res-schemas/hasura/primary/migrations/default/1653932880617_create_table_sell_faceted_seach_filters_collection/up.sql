CREATE TABLE "sell"."faceted_seach_filters_collection" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "title" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));COMMENT ON TABLE "sell"."faceted_seach_filters_collection" IS E'Filter used in faceted search within the Ecommerce Collection.';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
