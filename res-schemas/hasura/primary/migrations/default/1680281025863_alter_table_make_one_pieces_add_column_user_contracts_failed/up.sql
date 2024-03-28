
ALTER TABLE "make"."one_pieces" 
ADD COLUMN IF NOT EXISTS "user_contracts_failed" jsonb NULL;


ALTER TABLE "make"."one_pieces" 
ADD COLUMN IF NOT EXISTS  "user_defects" jsonb NULL DEFAULT jsonb_build_array();


 

