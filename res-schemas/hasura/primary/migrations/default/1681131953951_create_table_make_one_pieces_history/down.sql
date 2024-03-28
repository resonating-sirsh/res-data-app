-- Drop the one_pieces_history table only if it exists
DROP TABLE IF EXISTS make.one_pieces_history;

ALTER TABLE make.one_pieces DROP COLUMN IF EXISTS set_id;

--1681131953951_create_table_make_one_pieces_history/down.sql