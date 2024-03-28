
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "airtable_trim_taxonomy_id" text
--  null;

alter table "meta"."trims"
  add constraint "trims_trim_taxonomy_id_fkey"
  foreign key (trim_taxonomy_id)
  references "meta"."trim_taxonomy"
  (id) on update restrict on delete restrict;
alter table "meta"."trims" alter column "trim_taxonomy_id" drop not null;
alter table "meta"."trims" add column "trim_taxonomy_id" uuid;

alter table "meta"."trims"
  add constraint "trims_size_id_fkey"
  foreign key ("size_id")
  references "meta"."sizes"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims" drop constraint "trims_size_id_fkey";

alter table "meta"."trims" drop constraint "trims_trim_taxonomy_id_fkey",
  add constraint "trims_trim_taxonomy_id_fkey"
  foreign key ("trim_taxonomy_id")
  references "meta"."trim_taxonomy"
  ("id") on update restrict on delete restrict;

alter table "meta"."trims" drop constraint "trims_brand_id_fkey",
  add constraint "trims_brand_id_fkey"
  foreign key ("brand_id")
  references "sell"."brands"
  ("id") on update restrict on delete restrict;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "size_id" uuid
--  null;

alter table "meta"."trims" alter column "size_id" drop not null;
alter table "meta"."trims" add column "size_id" int4;

alter table "meta"."trims" drop constraint "trims_trim_taxonomy_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "trim_taxonomy_id" uuid
--  null;

alter table "meta"."trims" alter column "trim_taxonomy_id" drop not null;
alter table "meta"."trims" add column "trim_taxonomy_id" int4;

alter table "meta"."trims" drop constraint "trims_color_id_fkey";

alter table "meta"."trims" drop constraint "trims_brand_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "brand_id" integer
--  null;

alter table "meta"."trims" alter column "brand_id" drop not null;
alter table "meta"."trims" add column "brand_id" uuid;

DROP TABLE "meta"."trim_taxonomy";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "trim_taxonomy_id" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "expected_delivery_date" timestamptz
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "size_id" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "price_to_brand" numeric
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "resonance_trim_cost" numeric
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "order_quantity" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "trim_node_quantity" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "warehouse_quantity" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "available_quantity" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "image_url" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "color_id" uuid
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "supplied_type" text
--  null;

alter table "meta"."trims" alter column "sypplied_type" drop not null;
alter table "meta"."trims" add column "sypplied_type" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "sypplied_type" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "type" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "status" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "brand_id" UUID
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "name" text
--  null;
