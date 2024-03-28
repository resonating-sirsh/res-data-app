CREATE TABLE IF NOT EXISTS infraestructure.entity_ids (
    entity_name varchar(255) NOT NULL,
    last_id bigint NOT NULL DEFAULT 0,
    CONSTRAINT entity_ids_pkey PRIMARY KEY (entity_name)
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.constraint_column_usage
        WHERE table_name = 'transaction_details' AND constraint_name = 'transaction_details_type_fkey'
    ) THEN
        -- Drop the constraint if it exists
        ALTER TABLE sell.transaction_details DROP CONSTRAINT transaction_details_type_fkey;
    END IF;
END$$;


CREATE OR REPLACE FUNCTION infraestructure.get_next_entity_id(p_entity_name character varying)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_next_id BIGINT;
BEGIN
    -- Attempt to insert a new entity_name or update the last_id if it already exists
    INSERT INTO infraestructure.entity_ids (entity_name, last_id)
    VALUES (p_entity_name, 1)
    ON CONFLICT (entity_name) DO UPDATE
    SET last_id = infraestructure.entity_ids.last_id + 1
    RETURNING last_id INTO v_next_id;

    -- Return the next id
    RETURN v_next_id;
END;
$function$
;




ALTER TABLE sell.transactions 
ADD COLUMN IF NOT EXISTS payment_aggregate_id int,
ADD COLUMN  IF NOT EXISTS payment_intent_id text,
ADD COLUMN  IF NOT EXISTS stripe_customer_id text,
ADD COLUMN  IF NOT EXISTS stripe_last4 text,
ADD COLUMN  IF NOT EXISTS metadata jsonb,
ADD COLUMN  IF NOT EXISTS brand_code text;

ALTER TABLE sell.order_line_items
ADD COLUMN IF NOT EXISTS basic_cost_of_one_payment_stamped_at timestamp,
ADD COLUMN IF NOT EXISTS price_payment_stamped_at timestamp,
ADD COLUMN IF NOT EXISTS hold_entered_at timestamp,
ADD COLUMN IF NOT EXISTS hold_released_at timestamp;

ALTER TABLE sell.orders
ADD COLUMN IF NOT EXISTS payment_successful_at timestamp,
ADD COLUMN IF NOT EXISTS payment_aggregation_id int,
ADD COLUMN IF NOT EXISTS payment_stripe_charge_id text;