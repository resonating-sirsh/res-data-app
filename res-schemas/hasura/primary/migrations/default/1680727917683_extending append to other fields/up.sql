
CREATE OR REPLACE FUNCTION make.one_piece_costs_append_on_update()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL AS $BODY$
 BEGIN
  NEW."material_consumption"= OLD."material_consumption" || NEW."material_consumption";
  NEW."ink_consumption"= OLD."ink_consumption" || NEW."ink_consumption";
  NEW."electricity_consumption"= OLD."electricity_consumption" || NEW."electricity_consumption";
  NEW."water_consumption"= OLD."water_consumption" || NEW."water_consumption";
  RETURN NEW;
 END; $BODY$;

CREATE OR REPLACE FUNCTION make.one_piece_costs_append_on_update()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL AS $BODY$
 BEGIN
  NEW."material_consumption"= OLD."material_consumption" || NEW."material_consumption";
  NEW."ink_consumption"= OLD."ink_consumption" || NEW."ink_consumption";
  NEW."electricity_consumption"= OLD."electricity_consumption" || NEW."electricity_consumption";
  NEW."water_consumption"= OLD."water_consumption" || NEW."water_consumption";
  NEW."metadata"= OLD."metadata" || NEW."metadata";
  RETURN NEW;
 END; $BODY$;
