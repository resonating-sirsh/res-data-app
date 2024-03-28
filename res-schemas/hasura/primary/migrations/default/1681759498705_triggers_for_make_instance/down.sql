

CREATE TRIGGER "on_one_instance_ranking"
BEFORE INSERT ON "make"."one_orders"
FOR EACH ROW EXECUTE FUNCTION one_make_instance_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION one_make_instance_rank () RETURNS trigger AS $$
-- BEGIN
--      NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.one_orders WHERE one_code = NEW.one_code );
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_one_instance_ranking on make.one_orders;
-- CREATE TRIGGER on_one_instance_ranking
--   BEFORE INSERT ON make.one_orders
--   FOR EACH ROW
--   EXECUTE PROCEDURE one_make_instance_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION one_make_instance_rank () RETURNS trigger AS $$
-- BEGIN
--      NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.one_orders WHERE one_code = NEW.one_code );
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_one_instance_ranking on make.one_orders;
-- CREATE TRIGGER on_one_instance_ranking
--   BEFORE INSERT OR UPDATE ON make.one_orders
--   FOR EACH ROW
--   EXECUTE PROCEDURE one_make_instance_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION one_make_instance_rank () RETURNS trigger AS $$
-- BEGIN
--      NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.one_orders WHERE one_code = NEW.one_code );
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_one_instance_ranking on make.one_orders;
-- CREATE TRIGGER on_one_instance_ranking
--   BEFORE INSERT ON make.one_orders
--   FOR EACH ROW
--   EXECUTE PROCEDURE one_make_instance_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION make_instance_rank () RETURNS trigger AS $$
-- BEGIN
--      NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.make_instance_counter WHERE one_code = NEW.one_code );
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_make_instance_ranking on make.make_instance_counter;
-- CREATE TRIGGER on_make_instance_ranking
--   BEFORE INSERT ON make.make_instance_counter
--   FOR EACH ROW
--   EXECUTE PROCEDURE make_instance_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."make_instance_counter" add column "metadata" jsonb
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION make_instance_rank () RETURNS trigger AS $$
-- BEGIN
--      NEW.make_increment := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM make.make_instance_counter WHERE one_code = NEW.one_code );
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_make_instance_ranking on make.make_instance_counter;
-- CREATE TRIGGER on_make_instance_ranking
--   BEFORE INSERT ON make.make_instance_counter
--   FOR EACH ROW
--   EXECUTE PROCEDURE make_instance_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION make_instance_rank () RETURNS trigger AS $$
-- BEGIN
--      NEW.make_increment := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM make.make_instance_counter WHERE one_code = NEW.one_code );
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

comment on column "make"."make_instance_counter"."style_group" is E'attempted log of the  uniquness and instances of things we make';
alter table "make"."make_instance_counter" alter column "style_group" drop not null;
alter table "make"."make_instance_counter" add column "style_group" text;

comment on column "make"."make_instance_counter"."style_rank" is E'attempted log of the  uniquness and instances of things we make';
alter table "make"."make_instance_counter" alter column "style_rank" drop not null;
alter table "make"."make_instance_counter" add column "style_rank" text;

comment on column "make"."make_instance_counter"."customization" is E'attempted log of the  uniquness and instances of things we make';
alter table "make"."make_instance_counter" alter column "customization" set default jsonb_build_object();
alter table "make"."make_instance_counter" alter column "customization" drop not null;
alter table "make"."make_instance_counter" add column "customization" jsonb;

comment on column "make"."make_instance_counter"."style_id" is E'attempted log of the  uniquness and instances of things we make';
alter table "make"."make_instance_counter" alter column "style_id" drop not null;
alter table "make"."make_instance_counter" add column "style_id" uuid;

alter table "make"."one_pieces" drop constraint "one_pieces_one_order_id_fkey",
  add constraint "one_pieces_one_order_id_fkey"
  foreign key ("one_order_id")
  references "make"."one_orders"
  ("id") on update restrict on delete restrict;
