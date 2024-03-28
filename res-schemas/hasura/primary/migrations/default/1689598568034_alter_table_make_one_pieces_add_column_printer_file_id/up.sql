

ALTER TABLE make.one_pieces
    ADD COLUMN IF NOT EXISTS printer_file_id UUID null,
    
    ADD COLUMN IF NOT EXISTS roll_name text null,
    
    ADD COLUMN IF NOT EXISTS nest_key text null,
    
    ADD COLUMN IF NOT EXISTS roll_key text null,
    
    ADD COLUMN IF NOT EXISTS print_job_name text null,
    
    ADD COLUMN IF NOT EXISTS material_code text NULL;
    


    -- Up Migration for make.one_pieces_history

-- Add columns if they don't exist and make them nullable
ALTER TABLE make.one_pieces_history
    ADD COLUMN IF NOT EXISTS defects jsonb  NULL,
    
    ADD COLUMN IF NOT EXISTS contracts_failed jsonb NULL,
    
    ADD COLUMN IF NOT EXISTS user_defects jsonb NULL,
    
    ADD COLUMN IF NOT EXISTS user_contracts_failed jsonb NULL,
    
    ADD COLUMN IF NOT EXISTS metadata json NULL,
    
    ADD COLUMN IF NOT EXISTS printer_file_id UUID NULL,
    
    ADD COLUMN IF NOT EXISTS roll_name text NULL,
    
    ADD COLUMN IF NOT EXISTS nest_key text NULL,
    
    ADD COLUMN IF NOT EXISTS roll_key text NULL,
    
    ADD COLUMN IF NOT EXISTS print_job_name text NULL,
    
    ADD COLUMN IF NOT EXISTS material_code text  NULL;
    
