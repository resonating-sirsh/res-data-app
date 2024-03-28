alter table "digital_product"."digital_product_request"
  add constraint "digital_product_request_brand_id_fkey"
  foreign key ("brand_id")
  references "digital_product"."brands_wallets"
  ("id") on update restrict on delete restrict;
