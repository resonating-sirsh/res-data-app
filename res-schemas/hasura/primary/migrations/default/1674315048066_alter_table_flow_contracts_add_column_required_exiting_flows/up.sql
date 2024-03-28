alter table "flow"."contracts" add column "required_exiting_flows" jsonb
 null default jsonb_build_array();
