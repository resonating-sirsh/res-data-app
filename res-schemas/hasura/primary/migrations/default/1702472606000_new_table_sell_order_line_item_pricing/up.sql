CREATE TABLE IF NOT EXISTS sell.order_line_item_pricing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NULL,
    make_cost NUMERIC NOT NULL,
    retail_price NUMERIC NOT NULL,
    shipping NUMERIC NOT NULL,
    order_number TEXT NOT NULL,
    order_date TIMESTAMP WITH TIME ZONE NULL,
    order_type TEXT NOT NULL,
    revenue_share_amount NUMERIC NOT NULL,
    revenue_share_percentage NUMERIC NOT NULL,
    total_amount NUMERIC NOT NULL,
    sku TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_number ON sell.order_line_item_pricing (order_number);
