
alter table "meta"."trims" add column "name" text
 null;

alter table "meta"."trims" add column "brand_id" UUID
 null;

alter table "meta"."trims" add column "status" text
 null;

alter table "meta"."trims" add column "type" text
 null;

alter table "meta"."trims" add column "sypplied_type" text
 null;

alter table "meta"."trims" drop column "sypplied_type" cascade;

alter table "meta"."trims" add column "supplied_type" text
 null;

alter table "meta"."trims" add column "color_id" uuid
 null;

alter table "meta"."trims" add column "image_url" text
 null;

alter table "meta"."trims" add column "available_quantity" integer
 null;

alter table "meta"."trims" add column "warehouse_quantity" integer
 null;

alter table "meta"."trims" add column "trim_node_quantity" integer
 null;

alter table "meta"."trims" add column "order_quantity" integer
 null;

alter table "meta"."trims" add column "resonance_trim_cost" numeric
 null;

alter table "meta"."trims" add column "price_to_brand" numeric
 null;

alter table "meta"."trims" add column "size_id" integer
 null;

alter table "meta"."trims" add column "expected_delivery_date" timestamptz
 null;

alter table "meta"."trims" add column "trim_taxonomy_id" integer
 null;

CREATE TABLE "meta"."trim_taxonomy" ("id" UUID, "name" text, "type" text, "friendly_name" text NOT NULL, "size_id" uuid NOT NULL, "parent_trim_taxonomy_id" UUID NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("size_id") REFERENCES "meta"."sizes"("id") ON UPDATE restrict ON DELETE restrict, UNIQUE ("id"));COMMENT ON TABLE "meta"."trim_taxonomy" IS E'This represent the taxonomy of Trims';

alter table "meta"."trims" drop column "brand_id" cascade;

alter table "meta"."trims" add column "brand_id" integer
 null;

alter table "meta"."trims"
  add constraint "trims_brand_id_fkey"
  foreign key ("brand_id")
  references "sell"."brands"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims"
  add constraint "trims_color_id_fkey"
  foreign key ("color_id")
  references "meta"."colors"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims" drop column "trim_taxonomy_id" cascade;

alter table "meta"."trims" add column "trim_taxonomy_id" uuid
 null;

alter table "meta"."trims"
  add constraint "trims_trim_taxonomy_id_fkey"
  foreign key ("trim_taxonomy_id")
  references "meta"."trim_taxonomy"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims" drop column "size_id" cascade;

alter table "meta"."trims" add column "size_id" uuid
 null;

alter table "meta"."trims" drop constraint "trims_brand_id_fkey",
  add constraint "trims_brand_id_fkey"
  foreign key ("brand_id")
  references "sell"."brands"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims" drop constraint "trims_trim_taxonomy_id_fkey",
  add constraint "trims_trim_taxonomy_id_fkey"
  foreign key ("trim_taxonomy_id")
  references "meta"."trim_taxonomy"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims"
  add constraint "trims_size_id_fkey"
  foreign key ("size_id")
  references "meta"."sizes"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims" drop constraint "trims_size_id_fkey";

alter table "meta"."trims" drop column "trim_taxonomy_id" cascade;

alter table "meta"."trims" add column "airtable_trim_taxonomy_id" text
 null;
