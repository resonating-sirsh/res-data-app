

CREATE TABLE IF NOT EXISTS "meta"."edges" ("id" uuid NOT NULL, "piece_id" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("piece_id") REFERENCES "meta"."pieces"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "meta"."edges" IS E'inforamtion about edges on a pieces such as seam codes or seam allowances';

alter table "meta"."styles" add column "meta_one_uri" text
 null;

CREATE TABLE "sell"."products" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "style_id" uuid NOT NULL, "sku" text NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("style_id") REFERENCES "meta"."styles"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "sell"."products" IS E'ecommerce products and how we link them to meta products';
CREATE OR REPLACE FUNCTION "sell"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_sell_products_updated_at"
BEFORE UPDATE ON "sell"."products"
FOR EACH ROW
EXECUTE PROCEDURE "sell"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_sell_products_updated_at" ON "sell"."products" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;
