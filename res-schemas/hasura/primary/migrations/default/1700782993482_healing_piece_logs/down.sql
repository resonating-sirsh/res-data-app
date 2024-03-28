
comment on column "make"."one_piece_healing_requests"."inspected_at" is E'records every unqiue request by unique request if to heal a piece';
alter table "make"."one_piece_healing_requests" alter column "inspected_at" drop not null;
alter table "make"."one_piece_healing_requests" add column "inspected_at" timestamptz;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_piece_healing_requests" add column "print_asset_piece_id" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "make"."one_piece_healing_requests" add column "inspected_at" timestamptz
--  null;
