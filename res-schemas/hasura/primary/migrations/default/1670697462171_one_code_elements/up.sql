
CREATE TABLE "infraestructure"."bridge_fulfillments_and_ones" ("id" serial NOT NULL, "one_number" text NOT NULL, "order_item_fulfillment_id" uuid NOT NULL, "status" text NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") , FOREIGN KEY ("order_item_fulfillment_id") REFERENCES "sell"."order_item_fulfillments"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "infraestructure"."bridge_fulfillments_and_ones" IS E'a bridge between our fulfillment id and legacy one numners';
CREATE OR REPLACE FUNCTION "infraestructure"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_infraestructure_bridge_fulfillments_and_ones_updated_at"
BEFORE UPDATE ON "infraestructure"."bridge_fulfillments_and_ones"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_bridge_fulfillments_and_ones_updated_at" ON "infraestructure"."bridge_fulfillments_and_ones" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';

alter table "meta"."styles" add column "style_birthday" timestamptz
 null;

CREATE TABLE "make"."one_code_counter" ("created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "style_group" text NOT NULL, "style_rank" text NOT NULL, "id" uuid NOT NULL DEFAULT gen_random_uuid(), "customization" jsonb NOT NULL DEFAULT jsonb_build_object(), "style_id" uuid NOT NULL, PRIMARY KEY ("id") );COMMENT ON TABLE "make"."one_code_counter" IS E'attempted log of the  uniquness and instances of things we make';
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
CREATE TRIGGER "set_make_one_code_counter_updated_at"
BEFORE UPDATE ON "make"."one_code_counter"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_one_code_counter_updated_at" ON "make"."one_code_counter" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "make"."one_code_counter" add column "make_instance" integer
 null;

alter table "make"."one_code_counter" rename to "make_instance_counter";


CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name from meta.styles   
                    ) as x where x.name = NEW.name);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
CREATE TRIGGER on_style_for_ranking
  BEFORE INSERT OR UPDATE ON meta.styles
  FOR EACH ROW
  EXECUTE PROCEDURE style_rank();
