
alter table "meta"."materials" alter column "offset_size_inches" set not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."materials" add column "pretreatment_type" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."materials" add column "material_taxonomy" text
--  null;

alter table "meta"."materials" rename column "key" to "ket";

DROP TABLE "meta"."materials";

DROP TABLE "meta"."sizes";

DROP TABLE "meta"."piece_names";

DROP TABLE "meta"."piece_components";
