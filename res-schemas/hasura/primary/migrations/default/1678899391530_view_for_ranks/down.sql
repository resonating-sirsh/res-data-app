
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--      NEW.rank := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM meta.pieces_hash_registry WHERE body_code = NEW.body_code );
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM meta.pieces_hash_registry WHERE rank = NEW.rank );
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM meta.pieces_hash_registry WHERE rank = NEW.rank );
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     UPDATE meta.pieces_hash_registry
--       SET rank = b.r
--       FROM meta.pieces_hash_registry  a
--           JOIN meta.view_ranked_pieces_hashes b on a.id = b.id
--       WHERE a.id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();


CREATE TRIGGER "on_pieces_hash_for_ranking"
AFTER INSERT ON "meta"."pieces_hash_registry"
FOR EACH ROW EXECUTE FUNCTION pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE VIEW meta.view_ranked_pieces_hashes AS
--  SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r,
--   id, body_code from meta.pieces_hash_registry;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP VIEW "public"."ranked_pieces_hashes";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE VIEW ranked_pieces_hashes AS
--  SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r,
--   id, body_code from meta.pieces_hash_registry;


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--     IF NEW.rank IS NULL THEN
--         NEW.rank:=  (SELECT count(body_code) + 1 as r from meta.pieces_hash_registry where body_code = NEW.body_code);
--     END IF;
--
--     UPDATE meta.pieces_hash_registry
--      set rank = NEW.rank
--       where id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT COALESCE(r,1) from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--     UPDATE meta.pieces_hash_registry
--      set rank = NEW.rank
--       where id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--     IF NEW.rank IS NULL THEN
--         NEW.rank := (SELECT r from (
--                        SELECT max(rank)+1 as r  from meta.pieces_hash_registry  WHERE body_code = NEW.body_code
--                     ) as y );
--
--     END IF;
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--     IF NEW.rank IS NULL THEN
--         NEW.rank := (SELECT r from (
--                        SELECT max(rank)+1 as r  from meta.pieces_hash_registry  WHERE body_code = NEW.body_code
--                     ) as y );
--
--     END IF;
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY body_code ORDER BY created_at ASC) as r, id, body_code from meta.pieces_hash_registry
--                     ) as y where y.id = NEW.id and y.body_code = NEW.body_code);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

alter table "meta"."pieces_hash_registry" rename column "body_code" to "group";


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry  where id = NEW.id
--                     ) as record);
--
--     UPDATE meta.pieces_hash_registry SET rank = NEW.rank WHERE id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry  where id = NEW.id
--                     ) as record);
--
--     UPDATE meta.pieces_hash_registry SET rank = NEW.rank WHERE id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   WHEN (pg_trigger_depth() < 1)
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry  where id = NEW.id
--                     ) as record);
--
--     UPDATE meta.pieces_hash_registry SET rank = NEW.rank WHERE id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry  where id = NEW.id
--                     ) as record);
--
--     UPDATE meta.pieces_hash_registry SET rank = NEW.rank WHERE id = NEW.id;
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry  where id = NEW.id
--                     ) as record);
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (select r from (
--            SELECT  rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry   where id = NEW.id
--         ) as a);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--
--     NEW.rank := (select r from (
--            SELECT  rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry
--         ) as a);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     OLD.rank := (select r from (
--            SELECT  rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry   where id = NEW.id
--         ) as a);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (select r from (
--            SELECT  rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry   where id = NEW.id
--         ) as a);
--
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry
--                     ) as x where x.id = NEW.id);
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS pieces_hash_rank on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   AFTER INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

CREATE TRIGGER "on_pieces_hash_for_ranking"
BEFORE INSERT ON "meta"."pieces_hash_registry"
FOR EACH ROW EXECUTE FUNCTION pieces_hash_rank();


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry
--                     ) as x where x.id = NEW.id);
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;
--
-- DROP TRIGGER IF EXISTS pieces_hash_rank on meta.pieces_hash_registry;
-- CREATE TRIGGER on_pieces_hash_for_ranking
--   BEFORE INSERT OR UPDATE ON meta.pieces_hash_registry
--   FOR EACH ROW
--   EXECUTE PROCEDURE pieces_hash_rank();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, id from meta.pieces_hash_registry
--                     ) as x where x.id = NEW.id);
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
-- BEGIN
--     NEW.rank := (SELECT r from (
--                     SELECT rank() over (PARTITION BY "group" ORDER BY created_at ASC) as r, name from meta.pieces_hash_registry
--                     ) as x where x.id = NEW.id);
--   RETURN NEW;
-- END; $$ LANGUAGE plpgsql;

alter table "meta"."pieces_hash_registry" alter column "rank" set not null;
