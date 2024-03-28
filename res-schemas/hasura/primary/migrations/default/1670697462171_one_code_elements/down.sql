
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name from meta.styles
--                     ) as x where x.name = NEW.name);
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- --SELECT style_rank ()
--
-- DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
-- CREATE TRIGGER on_style_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.styles
--   FOR EACH ROW
--   EXECUTE PROCEDURE style_rank();
--
--
-- --) as q WHERE name = NEW.name
-- --SELECT * FROM meta.styles where name = 'The Classic Blouse - Great Lawn  in Silk Crepe de Chine'
-- --SELECT * FROM meta.styles where rank > 1;


alter table "make"."make_instance_counter" rename to "one_code_counter";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_code_counter" add column "make_instance" integer
--  null;

DROP TABLE "make"."one_code_counter";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."styles" add column "style_birthday" timestamptz
--  null;

DROP TABLE "infraestructure"."bridge_fulfillments_and_ones";
