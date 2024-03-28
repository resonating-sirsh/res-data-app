BEGIN;

ALTER TABLE dxa.flow_node_run RENAME TO job;
ALTER TABLE dxa.flow_node_config RENAME TO job_preset;
ALTER TABLE dxa.flow_node_run_status RENAME TO job_status;
ALTER TABLE dxa.flow_node RENAME TO job_type;

COMMIT;
