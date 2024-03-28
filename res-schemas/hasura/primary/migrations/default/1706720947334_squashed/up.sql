
alter table "meta"."trims" add column "start_date" timestamptz
 null default '2020-01-01T00:00:00';

alter table "meta"."trims" add column "end_date" timestamptz
 null default '9999-12-31T00:00:00';
