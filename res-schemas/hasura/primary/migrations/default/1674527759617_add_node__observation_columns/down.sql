
alter table "make"."piece_instances" rename column "roll_inspection_node_observation" to "roll_inspection_observation";

alter table "make"."piece_instances" rename column "roll_inspection_observation" to "roll_inspection_observatiob";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."piece_instances" add column "roll_inspection_observatiob" text
--  null default 'PENDING';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."piece_instances" add column "print_node_observation" text
--  null default 'PENDING';

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."print_jobs" add column "name" text
--  null;
