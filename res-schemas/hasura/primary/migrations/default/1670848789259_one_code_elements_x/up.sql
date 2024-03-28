

--trigger whenever we are inserting a style we need to set its rank
--we add to update to but it should be idempotent in practice and this is just for testing on existing styles
--for legacy we should use the source of truth from airtable but then we will always rank on our own created date which is safer
CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name, body_code from meta.styles   
                    ) as x where x.name = NEW.name and x.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
CREATE TRIGGER on_style_for_ranking
  BEFORE INSERT OR UPDATE ON meta.styles
  FOR EACH ROW
  EXECUTE PROCEDURE style_rank();


--) as q WHERE name = NEW.name
--SELECT * FROM meta.styles where name = 'The Classic Blouse - Great Lawn  in Silk Crepe de Chine'

--SELECT * FROM meta.styles where rank > 1

--SELECT * from hdb_catalog.schema_migrations;

--trigger whenever we are inserting a style we need to set its rank
--we add to update to but it should be idempotent in practice and this is just for testing on existing styles
--for legacy we should use the source of truth from airtable but then we will always rank on our own created date which is safer
CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY name, body_code ORDER BY style_birthday ASC) as r, name from meta.styles   
                    ) as x where x.name = NEW.name and x.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
CREATE TRIGGER on_style_for_ranking
  BEFORE INSERT OR UPDATE ON meta.styles
  FOR EACH ROW
  EXECUTE PROCEDURE style_rank();


--) as q WHERE name = NEW.name
--SELECT * FROM meta.styles where name = 'The Classic Blouse - Great Lawn  in Silk Crepe de Chine'

--SELECT * FROM meta.styles where rank > 1

--SELECT * from hdb_catalog.schema_migrations;

alter table "sell"."order_item_fulfillments"
  add constraint "order_item_fulfillments_order_item_id_fkey"
  foreign key ("order_item_id")
  references "sell"."order_line_items"
  ("id") on update restrict on delete restrict;

alter table "sell"."order_line_items" add column "fulfillable_quantity" integer
 null;
