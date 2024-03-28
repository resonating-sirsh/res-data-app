-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_pieces_history" add column "make_instance" integer
--  null;

ALTER TABLE make.one_pieces_history
    
    DROP COLUMN IF EXISTS make_instance;
