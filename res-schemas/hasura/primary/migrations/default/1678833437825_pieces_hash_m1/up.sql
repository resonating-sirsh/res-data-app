alter table "meta"."styles" add column "pieces_hash_ref" uuid
 null;

CREATE TABLE "meta"."pieces_hash_registry" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "group" text NOT NULL, "rank" integer NOT NULL, PRIMARY KEY ("id") );
CREATE OR REPLACE FUNCTION "meta"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_meta_pieces_hash_registry_updated_at"
BEFORE UPDATE ON "meta"."pieces_hash_registry"
FOR EACH ROW
EXECUTE PROCEDURE "meta"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_meta_pieces_hash_registry_updated_at" ON "meta"."pieces_hash_registry" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "meta"."styles" rename column "pieces_hash_ref" to "pieces_hash_id";

alter table "meta"."styles"
  add constraint "styles_pieces_hash_id_fkey"
  foreign key ("pieces_hash_id")
  references "meta"."pieces_hash_registry"
  ("id") on update restrict on delete restrict;
