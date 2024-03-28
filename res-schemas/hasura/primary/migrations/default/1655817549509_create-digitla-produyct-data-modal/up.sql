
create schema "digital_product";
CREATE TABLE "digital_product"."brands_wallets" ("id" text NOT NULL, "wallet_address" text NOT NULL, "brand_code" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("wallet_address"), UNIQUE ("id"));COMMENT ON TABLE "digital_product"."brands_wallets" IS E'This represents Brands Addresses from the digital products perspective.';

CREATE TABLE "digital_product"."digital_product_request" ("id" serial NOT NULL, "brand_id" text NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "style_code" text NOT NULL, "nft_address" text NOT NULL, "ipfs_location" text NOT NULL, "trasnfered_at" timestamptz NOT NULL, "gas_fee" bigint NOT NULL, "processing_fee" integer NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("brand_id") REFERENCES "digital_product"."brands_wallets"("id") ON UPDATE restrict ON DELETE restrict, UNIQUE ("id"), UNIQUE ("style_code"), UNIQUE ("nft_address"), UNIQUE ("ipfs_location"));COMMENT ON TABLE "digital_product"."digital_product_request" IS E'This records represent NTFs on the Ethereum blockchain.';
CREATE OR REPLACE FUNCTION "digital_product"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_digital_product_digital_product_request_updated_at"
BEFORE UPDATE ON "digital_product"."digital_product_request"
FOR EACH ROW
EXECUTE PROCEDURE "digital_product"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_digital_product_digital_product_request_updated_at" ON "digital_product"."digital_product_request" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE TABLE "digital_product"."digital_product_request_type" ("name" text NOT NULL, "description" text NOT NULL, PRIMARY KEY ("name") , UNIQUE ("name"));
alter table "digital_product"."digital_product_request" add column "type" text
 null;
alter table "digital_product"."digital_product_request"
  add constraint "digital_product_request_type_fkey"
  foreign key ("type")
  references "digital_product"."digital_product_request_type"
  ("name") on update restrict on delete restrict;
alter table "digital_product"."digital_product_request" add column "animation_id" uuid
 null;

alter table "digital_product"."digital_product_request" add column "thumbnail_id" uuid
 null;

alter table "digital_product"."digital_product_request"
  add constraint "digital_product_request_animation_id_fkey"
  foreign key ("animation_id")
  references "create"."asset"
  ("id") on update cascade on delete cascade;

alter table "digital_product"."digital_product_request"
  add constraint "digital_product_request_thumbnail_id_fkey"
  foreign key ("thumbnail_id")
  references "create"."asset"
  ("id") on update cascade on delete cascade;

INSERT 
    INTO "digital_product"."digital_product_request_type"
    VALUES ('created', 'Mint a new NFT')
;

INSERT 
    INTO "digital_product"."digital_product_request_type"
    VALUES ('transfer', 'Transfer NFT from resonances dapps to brands wallets')
;

CREATE TABLE "digital_product"."digital_product_requests_status" ("name" text NOT NULL, "comment" text, PRIMARY KEY ("name") , UNIQUE ("name"));COMMENT ON TABLE "digital_product"."digital_product_requests_status" IS E'Enum table which specify all of the status of a digital_product_requests_status';

alter table "digital_product"."digital_product_requests_status" rename to "digital_product_request_statu";

INSERT
    INTO "digital_product"."digital_product_request_statu"
    VALUES
        ('processing', 'The request is being processing'),
        ('done', 'The request has fulfilled correctly'),
        ('error', 'Something fail during processing the record'),
        ('hold', 'The request is waiting to be processed'),
        ('retry', 'The request is going to be tried again')
;

alter table "digital_product"."digital_product_request" drop column "nft_address" cascade;

alter table "digital_product"."digital_product_request" add column "costing" json
 null;

alter table "digital_product"."digital_product_request" drop column "processing_fee" cascade;

alter table "digital_product"."digital_product_request" drop column "gas_fee" cascade;

alter table "digital_product"."digital_product_request" rename column "trasnfered_at" to "transfered_at";

alter table "digital_product"."digital_product_request" add column "status" text
 not null default 'hold';

alter table "digital_product"."digital_product_request"
  add constraint "digital_product_request_status_fkey"
  foreign key ("status")
  references "digital_product"."digital_product_request_statu"
  ("name") on update no action on delete no action;

alter table "digital_product"."digital_product_request" alter column "ipfs_location" drop not null;

alter table "digital_product"."digital_product_request" alter column "transfered_at" drop not null;

alter table "digital_product"."digital_product_request" alter column "type" set not null;

alter table "digital_product"."digital_product_request" alter column "animation_id" set not null;

alter table "digital_product"."digital_product_request" alter column "thumbnail_id" set not null;
