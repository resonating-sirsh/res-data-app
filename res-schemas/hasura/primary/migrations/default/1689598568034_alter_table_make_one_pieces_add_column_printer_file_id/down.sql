
-- Remove added columns and make them not nullable
ALTER TABLE make.one_pieces
    
    DROP COLUMN IF EXISTS printer_file_id,
    
    DROP COLUMN IF EXISTS roll_name,
    
    DROP COLUMN IF EXISTS nest_key,
    
    DROP COLUMN IF EXISTS roll_key,
    
    DROP COLUMN IF EXISTS print_job_name,
    
    DROP COLUMN IF EXISTS material_code;
    



ALTER TABLE make.one_pieces_history
    
    DROP COLUMN IF EXISTS defects,
    
    DROP COLUMN IF EXISTS contracts_failed,
    
    DROP COLUMN IF EXISTS user_defects,
    
    DROP COLUMN IF EXISTS user_contracts_failed,
    
    DROP COLUMN IF EXISTS metadata,
    
    DROP COLUMN IF EXISTS printer_file_id,
    
    DROP COLUMN IF EXISTS roll_name,
  
    DROP COLUMN IF EXISTS nest_key,
  
    DROP COLUMN IF EXISTS roll_key,
   
    DROP COLUMN IF EXISTS print_job_name,
  
    DROP COLUMN IF EXISTS material_code;
