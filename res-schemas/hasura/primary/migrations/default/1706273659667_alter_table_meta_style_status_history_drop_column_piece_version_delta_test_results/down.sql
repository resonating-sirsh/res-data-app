comment on column "meta"."style_status_history"."piece_version_delta_test_results" is E'maintains status of styles by body version and what their status is - not a complete log of all states a style goes through';
alter table "meta"."style_status_history" alter column "piece_version_delta_test_results" set default jsonb_build_object();
alter table "meta"."style_status_history" alter column "piece_version_delta_test_results" drop not null;
alter table "meta"."style_status_history" add column "piece_version_delta_test_results" jsonb;
