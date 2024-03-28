alter table "meta"."styles" add column "piece_version_delta_test_results" jsonb
 null default jsonb_build_object();
