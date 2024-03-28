
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "piece_set_size" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "normed_size" text
--  null;

alter table "meta"."pieces" rename column "piece_ordinal" to "piece_index";

alter table "meta"."pieces" rename column "piece_index" to "rank";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "rank" integer
--  null;
