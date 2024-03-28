CREATE TABLE "dxa"."vs_plugin_validations" (
    "id" uuid NOT NULL DEFAULT gen_random_uuid(), 
    "created_at" timestamptz NOT NULL DEFAULT now(), 
    "garment_name" text not null,
    "username" text not null,
    "filename" text not null,
    "errors" jsonb not null DEFAULT jsonb_build_object(),
    PRIMARY KEY ("id") );
CREATE EXTENSION IF NOT EXISTS pgcrypto;
