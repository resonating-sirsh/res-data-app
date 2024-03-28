
alter table "sell"."orders" add column IF NOT exists "deleted_at"  timestamptz
 null;

CREATE TABLE "make"."one_piece_node_consumption" ("id" serial NOT NULL, "name" text NOT NULL, "units" text NOT NULL, "value" numeric NOT NULL, "node_id" uuid NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "one_piece_id" uuid NOT NULL, PRIMARY KEY ("id") );
CREATE OR REPLACE FUNCTION "make"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_make_one_piece_node_consumption_updated_at"
BEFORE UPDATE ON "make"."one_piece_node_consumption"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_piece_node_consumption_updated_at" ON "make"."one_piece_node_consumption" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "make"."one_piece_node_consumption" rename to "one_piece_at_node_consumption";

alter table "make"."one_piece_at_node_consumption" add column "category" text
 null;

alter table "make"."one_piece_at_node_consumption" rename column "id" to "ids";

alter table "make"."one_piece_at_node_consumption" add column "id" uuid
 null;

alter table "make"."one_piece_at_node_consumption" alter column "ids" set default '0';

BEGIN TRANSACTION;
ALTER TABLE "make"."one_piece_at_node_consumption" DROP CONSTRAINT "one_piece_node_consumption_pkey";

ALTER TABLE "make"."one_piece_at_node_consumption"
    ADD CONSTRAINT "one_piece_node_consumption_pkey" PRIMARY KEY ("id");
COMMIT TRANSACTION;

alter table "make"."one_piece_at_node_consumption" drop column "ids" cascade;

alter table "make"."one_piece_at_node_consumption"
  add constraint "one_piece_at_node_consumption_id_fkey"
  foreign key ("id")
  references "make"."one_pieces"
  ("oid") on update restrict on delete restrict;

alter table "make"."one_piece_at_node_consumption"
  add constraint "one_piece_at_node_consumption_node_id_fkey"
  foreign key ("node_id")
  references "flow"."nodes"
  ("id") on update restrict on delete restrict;

alter table "make"."one_piece_at_node_consumption" alter column "one_piece_id" drop not null;

alter table "make"."one_piece_at_node_consumption" alter column "one_piece_id" set not null;

alter table "make"."one_piece_at_node_consumption" drop constraint "one_piece_at_node_consumption_id_fkey",
  add constraint "one_piece_at_node_consumption_one_piece_id_fkey"
  foreign key ("one_piece_id")
  references "make"."one_pieces"
  ("oid") on update restrict on delete restrict;
