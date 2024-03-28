alter table "meta"."pieces_hash_registry" alter column "rank" drop not null;


CREATE OR REPLACE FUNCTION pieces_hash_rank () RETURNS trigger AS $$
BEGIN

     NEW.rank := ( SELECT COALESCE(MAX(rank), 0) + 1 FROM meta.pieces_hash_registry WHERE body_code = NEW.body_code );

  RETURN NEW;
END; $$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_pieces_hash_for_ranking on meta.pieces_hash_registry;
CREATE TRIGGER on_pieces_hash_for_ranking
  BEFORE INSERT ON meta.pieces_hash_registry
  FOR EACH ROW
  EXECUTE PROCEDURE pieces_hash_rank();
