
alter table "make"."one_pieces" drop constraint "one_pieces_one_order_id_fkey",
  add constraint "one_pieces_one_order_id_fkey"
  foreign key ("one_order_id")
  references "make"."one_orders"
  ("id") on update cascade on delete cascade;

alter table "make"."make_instance_counter" drop column "style_id" cascade;

alter table "make"."make_instance_counter" drop column "customization" cascade;

alter table "make"."make_instance_counter" drop column "style_rank" cascade;

alter table "make"."make_instance_counter" drop column "style_group" cascade;


CREATE OR REPLACE FUNCTION make_instance_rank () RETURNS trigger AS $$
BEGIN
     NEW.make_increment := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM make.make_instance_counter WHERE one_code = NEW.one_code );
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION make_instance_rank () RETURNS trigger AS $$
BEGIN
     NEW.make_increment := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM make.make_instance_counter WHERE one_code = NEW.one_code );
  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_make_instance_ranking on make.make_instance_counter;
CREATE TRIGGER on_make_instance_ranking
  BEFORE INSERT ON make.make_instance_counter
  FOR EACH ROW
  EXECUTE PROCEDURE make_instance_rank();

alter table "make"."make_instance_counter" add column "metadata" jsonb
 null;

CREATE OR REPLACE FUNCTION make_instance_rank () RETURNS trigger AS $$
BEGIN
     NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.make_instance_counter WHERE one_code = NEW.one_code );
  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_make_instance_ranking on make.make_instance_counter;
CREATE TRIGGER on_make_instance_ranking
  BEFORE INSERT ON make.make_instance_counter
  FOR EACH ROW
  EXECUTE PROCEDURE make_instance_rank();

CREATE OR REPLACE FUNCTION one_make_instance_rank () RETURNS trigger AS $$
BEGIN
     NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.one_orders WHERE one_code = NEW.one_code );
  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_one_instance_ranking on make.one_orders;
CREATE TRIGGER on_one_instance_ranking
  BEFORE INSERT ON make.one_orders
  FOR EACH ROW
  EXECUTE PROCEDURE one_make_instance_rank();

CREATE OR REPLACE FUNCTION one_make_instance_rank () RETURNS trigger AS $$
BEGIN
     NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.one_orders WHERE one_code = NEW.one_code );
  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_one_instance_ranking on make.one_orders;
CREATE TRIGGER on_one_instance_ranking
  BEFORE INSERT OR UPDATE ON make.one_orders
  FOR EACH ROW
  EXECUTE PROCEDURE one_make_instance_rank();

CREATE OR REPLACE FUNCTION one_make_instance_rank () RETURNS trigger AS $$
BEGIN
     NEW.make_instance := ( SELECT COALESCE(MAX(make_instance), 0) + 1 FROM make.one_orders WHERE one_code = NEW.one_code );
  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_one_instance_ranking on make.one_orders;
CREATE TRIGGER on_one_instance_ranking
  BEFORE INSERT ON make.one_orders
  FOR EACH ROW
  EXECUTE PROCEDURE one_make_instance_rank();

DROP TRIGGER "on_one_instance_ranking" ON "make"."one_orders";
