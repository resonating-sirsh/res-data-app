BEGIN;

ALTER TABLE dxa.job RENAME TO flow_node_run;
ALTER TABLE dxa.job_preset RENAME TO flow_node_config;
ALTER TABLE dxa.job_status RENAME TO flow_node_run_status;
ALTER TABLE dxa.job_type RENAME TO flow_node;

COMMIT;
