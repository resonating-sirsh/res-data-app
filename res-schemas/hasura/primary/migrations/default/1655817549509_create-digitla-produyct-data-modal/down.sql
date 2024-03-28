
alter table "digital_product"."digital_product_request" alter column "thumbnail_id" drop not null;

alter table "digital_product"."digital_product_request" alter column "animation_id" drop not null;

alter table "digital_product"."digital_product_request" alter column "type" drop not null;

alter table "digital_product"."digital_product_request" alter column "transfered_at" set not null;

alter table "digital_product"."digital_product_request" alter column "ipfs_location" set not null;

alter table "digital_product"."digital_product_request" drop constraint "digital_product_request_status_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "status" text
--  not null default 'hold';

alter table "digital_product"."digital_product_request" rename column "transfered_at" to "trasnfered_at";

comment on column "digital_product"."int8" is E'This records represent NTFs on the Ethereum blockchain.';
alter table "digital_product"."digital_product_request" alter column "gas_fee" drop not null;
alter table "digital_product"."digital_product_request" add column "gas_fee" int8;

comment on column "digital_product"."int4" is E'This records represent NTFs on the Ethereum blockchain.';
alter table "digital_product"."digital_product_request" alter column "processing_fee" drop not null;
alter table "digital_product"."digital_product_request" add column "processing_fee" int4;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "costing" json
--  null;

comment on column "digital_product"."text" is E'This records represent NTFs on the Ethereum blockchain.';
alter table "digital_product"."digital_product_request" add constraint "digital_product_request_nft_address_key" unique (nft_address);
alter table "digital_product"."digital_product_request" alter column "nft_address" drop not null;
alter table "digital_product"."digital_product_request" add column "nft_address" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
DELETE FROM "digital_product"."digital_product_request_statu" WHERE name='processing';
DELETE FROM "digital_product"."digital_product_request_statu" WHERE name='done';
DELETE FROM "digital_product"."digital_product_request_statu" WHERE name='error';
DELETE FROM "digital_product"."digital_product_request_statu" WHERE name='hold';
DELETE FROM "digital_product"."digital_product_request_statu" WHERE name='retry';

alter table "digital_product"."digital_product_request_statu" rename to "digital_product_requests_status";

DROP TABLE "digital_product"."digital_product_requests_status";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:

DELETE
    FROM "digital_product"."digital_product_request_type"
    WHERE name='created'
;

DELETE
    FROM "digital_product"."digital_product_request_type"
    WHERE name='transfer'
;


alter table "digital_product"."digital_product_request" drop constraint "digital_product_request_thumbnail_id_fkey";

alter table "digital_product"."digital_product_request" drop constraint "digital_product_request_animation_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "thumbnail_id" uuid
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "digital_product"."digital_product_request" add column "animation_id" uuid
--  null;

alter table "digital_product"."digital_product_request" drop constraint "digital_product_request_type_fkey";
alter table "digital_product"."digital_product_request"
drop column "type";
DROP TABLE "digital_product"."digital_product_request_type";
DROP TABLE "digital_product"."digital_product_request";
DROP TABLE "digital_product"."brands_wallets";
drop schema "digital_product" cascade;