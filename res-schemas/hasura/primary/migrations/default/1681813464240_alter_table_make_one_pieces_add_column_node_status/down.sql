
-- Drop the "node_status" column added to the "one_pieces" table in the last migration
ALTER TABLE "make"."one_pieces" DROP COLUMN IF EXISTS "status";