alter table "meta"."style_status_history" add column "piece_version_delta_test_results" jsonb
 null default jsonb_build_object();
