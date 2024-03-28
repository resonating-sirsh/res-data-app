
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."dead_letters" add column "counter" integer
--  null default '0';

alter table "infraestructure"."dead_letters" rename column "reprocessed_at" to "reprocessed_date";
comment on column "infraestructure"."dead_letters"."reprocessed_date" is NULL;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "infraestructure"."dead_letters" add column "reprocessed_date" timestamptz
--  null;

DROP TABLE "infraestructure"."dead_letters";
