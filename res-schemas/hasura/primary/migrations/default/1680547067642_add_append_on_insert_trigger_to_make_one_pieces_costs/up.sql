
CREATE OR REPLACE FUNCTION make.one_piece_costs_append_on_update()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL AS $BODY$
 BEGIN
  NEW."material_consumption"= OLD."material_consumption" || NEW."material_consumption";
  NEW."ink_consumption"= OLD."ink_consumption" || NEW."ink_consumption";
  RETURN NEW;
 END; $BODY$;

CREATE TRIGGER one_piece_costs_append_on_update
BEFORE UPDATE ON "make"."one_pieces_costs"
FOR EACH ROW EXECUTE PROCEDURE
make.one_piece_costs_append_on_update();
