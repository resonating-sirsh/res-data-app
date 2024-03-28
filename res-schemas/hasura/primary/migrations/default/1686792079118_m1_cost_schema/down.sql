
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "finance"."material_rates" add column "name" text
--  null;

ALTER TABLE "meta"."style_sizes" ALTER COLUMN "piece_count_checksum" TYPE numeric;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."style_sizes" add column "piece_count_checksum" numeric
--  null;

alter table "meta"."style_sizes" rename column "material_usage_statistics" to "material_usage";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."style_sizes" add column "material_usage" jsonb
--  null default jsonb_build_array();

alter table "meta"."bodies" rename column "estimated_sewing_time" to "estimated_sewing_costs";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "finance"."material_rates" add column "ended_at" timestamptz
--  null;

comment on column "finance"."material_rates"."ended_at" is E'material rate tracking history';
alter table "finance"."material_rates" alter column "ended_at" drop not null;
alter table "finance"."material_rates" add column "ended_at" numeric;

alter table "meta"."bodies" rename column "estimated_sewing_costs" to "sewing_estimated_costs";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "finance"."material_rates" add column "material_code" text
--  not null;

DROP TABLE "finance"."material_rates";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "finance"."node_rates" add column "ended_at" timestamptz
--  null;

DROP TABLE "finance"."node_rates";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "flow"."node_rates";

DROP TABLE "flow"."node_rates";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "flow"."finance_node_rates";

drop schema "finance" cascade;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."style_sizes" add column "size_yield" numeric
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."bodies" add column "sewing_estimated_costs" numeric
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."bodies" add column "trim_costs" numeric
--  null;
