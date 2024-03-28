BEGIN TRANSACTION;
ALTER TABLE "make"."roll_inspection" DROP CONSTRAINT "roll_inspection_pkey";

ALTER TABLE "make"."roll_inspection"
    ADD CONSTRAINT "roll_inspection_pkey" PRIMARY KEY ("piece_id", "nest_key", "inspector", "defect_idx");
COMMIT TRANSACTION;
