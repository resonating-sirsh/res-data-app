BEGIN TRANSACTION;
ALTER TABLE "make"."assemble_rolls" DROP CONSTRAINT "assemble_rolls_pkey";

ALTER TABLE "make"."assemble_rolls"
    ADD CONSTRAINT "assemble_rolls_pkey" PRIMARY KEY ("roll_primary_key");
COMMIT TRANSACTION;
