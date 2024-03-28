BEGIN;

CREATE SCHEMA IF NOT EXISTS "dxa";

ALTER TABLE "platform".job
SET SCHEMA "dxa";

ALTER TABLE "platform".job_preset
SET SCHEMA "dxa";

ALTER TABLE "platform".job_status
SET SCHEMA "dxa";

ALTER TABLE "platform".job_type
SET SCHEMA "dxa";

DROP SCHEMA IF EXISTS "platform" CASCADE;

COMMIT;
