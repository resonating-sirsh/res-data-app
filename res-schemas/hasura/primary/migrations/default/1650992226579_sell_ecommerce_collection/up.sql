
DROP table "sell"."ecommerce_collections_rules";

CREATE TABLE "sell"."ecommerce_collection_status" ("value" text NOT NULL, "comment" text, PRIMARY KEY ("value") , UNIQUE ("value"));COMMENT ON TABLE "sell"."ecommerce_collection_status" IS E'Ecommerce Collection Status';


insert into "sell"."ecommerce_collection_status" VALUES
  ('created', 'The collection is exported in Ecommerce but is not published in any Sale Channel'),
  ('draft', 'The collection is draft or it is archived in ecommerce'),
  ('delete', 'The collection is deleted in ecommerce and shouldnt be seen by the user in create.ONE'),
  ('unpublish', 'The collection is not live in any sales channel'),
  ('live', 'The collection is live in all sales channels');

alter table "sell"."ecommerce_collections" drop column "imported_at" cascade;

alter table "sell"."ecommerce_collections" add column "rules" json
 null;

alter table "sell"."ecommerce_collections" add column "disjuntive" boolean
 null;

alter table "sell"."ecommerce_collections" add column "status" text
 null;

alter table "sell"."ecommerce_collections"
  add constraint "ecommerce_collections_status_fkey"
  foreign key ("status")
  references "sell"."ecommerce_collection_status"
  ("value") on update cascade on delete cascade;
