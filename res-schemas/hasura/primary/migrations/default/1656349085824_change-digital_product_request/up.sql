alter table "digital_product"."digital_product_request" add column "token_id" text default '0';

alter table "digital_product"."digital_product_request" add column "to_address" text default '0x';

alter table "digital_product"."digital_product_request" add column "contract_address" text default '0x0'::text;
