create schema "make";

CREATE TABLE "make"."make_orders" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "one_number" text NOT NULL,
  "sku" text,
  "order_number" text,
  "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id") ,
  UNIQUE ("one_number")
);
COMMENT ON TABLE "make"."make_orders" IS
E'This is 1:1 with Meta ONEs, and M:1 with Fullfillment Order Line Items (i.e. up to quantity)';
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
CREATE TRIGGER "set_make_make_orders_updated_at"
BEFORE UPDATE ON "make"."make_orders"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_make_orders_updated_at" ON "make"."make_orders" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "make"."piece_sets" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "one_number" text NOT NULL,
  "key" text NOT NULL,
  "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  FOREIGN KEY ("one_number") REFERENCES "make"."make_orders"("one_number") ON UPDATE restrict ON DELETE restrict,
  UNIQUE("key")
);
COMMENT ON TABLE "make"."piece_sets" IS E'Collections of pieces for a make order from a specific sources, these are subsets of instantiated meta ones.';
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
CREATE TRIGGER "set_make_piece_sets_updated_at"
BEFORE UPDATE ON "make"."piece_sets"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_piece_sets_updated_at" ON "make"."piece_sets" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "make"."piece_instances" (
  "id" text NOT NULL,
  "one_number" text NOT NULL,
  "piece_set_id" uuid NOT NULL,
  "piece_set_key" text not null,
  "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id"),
  FOREIGN KEY ("one_number") REFERENCES "make"."make_orders"("one_number") ON UPDATE restrict ON DELETE restrict,
  FOREIGN KEY ("piece_set_id") REFERENCES "make"."piece_sets"("id") ON UPDATE restrict ON DELETE restrict,
  FOREIGN KEY ("piece_set_key") REFERENCES "make"."piece_sets"("key") ON UPDATE restrict ON DELETE restrict
);
COMMENT ON TABLE "make"."piece_instances" IS E'Pieces contained in piece sets.';
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
CREATE TRIGGER "set_make_piece_instances_updated_at"
BEFORE UPDATE ON "make"."piece_instances"
FOR EACH ROW
EXECUTE PROCEDURE "make"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_make_piece_instances_updated_at" ON "make"."piece_instances" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
