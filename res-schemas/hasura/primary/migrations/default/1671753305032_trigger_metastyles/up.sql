
CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name, body_code from meta.styles   
                    ) as y where y.name = NEW.name and y.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
CREATE TRIGGER on_style_for_ranking
  BEFORE INSERT OR UPDATE ON meta.styles
  FOR EACH ROW
  EXECUTE PROCEDURE style_rank();

CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name, body_code from meta.styles   
                    ) as y where y.name = NEW.name and y.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
-- CREATE TRIGGER on_style_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.styles
--   FOR EACH ROW
--   EXECUTE PROCEDURE style_rank();

CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name, body_code from meta.styles   
                    ) as y where y.name = NEW.name and y.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
-- CREATE TRIGGER on_style_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.styles
--   FOR EACH ROW
--   EXECUTE PROCEDURE style_rank();

CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name, body_code from meta.styles   
                    ) as y where y.name = NEW.name and y.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
-- CREATE TRIGGER on_style_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.styles
--   FOR EACH ROW
--   EXECUTE PROCEDURE style_rank();

CREATE OR REPLACE FUNCTION style_rank () RETURNS trigger AS $$
BEGIN
    NEW.rank := (SELECT r from (
                    SELECT rank() over (PARTITION BY body_code ORDER BY style_birthday ASC) as r, name, body_code from meta.styles   
                    ) as y where y.name = NEW.name and y.body_code = NEW.body_code);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS on_style_for_ranking on meta.styles;
CREATE TRIGGER on_style_for_ranking
  BEFORE INSERT OR UPDATE ON meta.styles
  FOR EACH ROW
  EXECUTE PROCEDURE style_rank();
