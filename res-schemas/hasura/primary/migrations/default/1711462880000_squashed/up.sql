DO $$
BEGIN
    -- Check for revenue_share_amount column in sell.order_line_items
    IF NOT EXISTS (
        SELECT FROM pg_attribute 
        WHERE  attrelid = 'sell.order_line_items'::regclass
        AND    attname   = 'revenue_share_amount'
        AND    attnum    > 0
        AND    NOT attisdropped
    ) THEN
        ALTER TABLE sell.order_line_items ADD COLUMN revenue_share_amount NUMERIC(12,2);
    END IF;

    -- Check for line_total_charge_amount column in sell.order_line_items
    IF NOT EXISTS (
        SELECT FROM pg_attribute 
        WHERE  attrelid = 'sell.order_line_items'::regclass
        AND    attname   = 'line_total_charge_amount'
        AND    attnum    > 0
        AND    NOT attisdropped
    ) THEN
        ALTER TABLE sell.order_line_items ADD COLUMN line_total_charge_amount NUMERIC(12,2);
    END IF;

    -- Repeat for other columns and tables as necessary
    -- Example for must_pay_before_make in sell.brands
    IF NOT EXISTS (
        SELECT FROM pg_attribute 
        WHERE  attrelid = 'sell.brands'::regclass
        AND    attname   = 'must_pay_before_make'
        AND    attnum    > 0
        AND    NOT attisdropped
    ) THEN
        ALTER TABLE sell.brands ADD COLUMN must_pay_before_make BOOL;
    END IF;

    -- Example for must_pay_before_make in sell.brands_archive
    IF NOT EXISTS (
        SELECT FROM pg_attribute 
        WHERE  attrelid = 'sell.brands_archive'::regclass
        AND    attname   = 'must_pay_before_make'
        AND    attnum    > 0
        AND    NOT attisdropped
    ) THEN
        ALTER TABLE sell.brands_archive ADD COLUMN must_pay_before_make BOOL;
    END IF;
END
$$;
