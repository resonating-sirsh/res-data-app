
alter table "flow"."contracts" rename column "owner_user_id" to "owner";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "flow"."actions";

DROP TABLE "flow"."actions";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."resources" add column "type" text
--  not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."resources" add column "node_id" uuid
--  not null;

DROP TABLE "flow"."resources";

DROP TABLE "flow"."edges";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "flow"."dags";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."nodes" add column "metadata" jsonb
--  not null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."contracts" add column "code" text
--  not null;

DROP TABLE "flow"."contracts";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "flow"."users" add column "metadata" jsonb
--  null default jsonb_build_object();

DROP TABLE "flow"."dags";

DROP TABLE "flow"."users";
