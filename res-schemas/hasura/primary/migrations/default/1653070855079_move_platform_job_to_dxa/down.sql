BEGIN;

CREATE SCHEMA IF NOT EXISTS "platform";

ALTER TABLE "dxa".job
SET SCHEMA "platform";

ALTER TABLE "dxa".job_preset
SET SCHEMA "platform";

ALTER TABLE "dxa".job_status
SET SCHEMA "platform";

ALTER TABLE "dxa".job_type
SET SCHEMA "platform";

DROP SCHEMA IF EXISTS "dxa" CASCADE;

COMMIT;
