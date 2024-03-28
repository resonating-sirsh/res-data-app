--CREATE sell.fulfillapi_orders

CREATE TABLE IF NOT EXISTS sell.fulfillapi_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    customer_id UUID,
    ecommerce_source TEXT,
    source_order_id TEXT NULL,
    ordered_at TIMESTAMPTZ,
    brand_code TEXT,
    source_metadata JSONB NULL,
    sales_channel TEXT NULL,
    order_channel TEXT NULL,
    email TEXT NULL,
    tags JSONB NULL,
    item_quantity_hash TEXT NULL,
    name TEXT NULL,
    status TEXT NULL,
    contracts_failing JSONB NULL DEFAULT jsonb_build_object(),
    deleted_at TIMESTAMPTZ NULL,
    was_payment_successful BOOLEAN DEFAULT false,
    was_balance_not_enough BOOLEAN DEFAULT false,
    address_1 TEXT NULL,
    address_2 TEXT NULL,
    revenue_share_percent NUMERIC NULL
);

-- Add foreign keys if they don't exist (assuming referenced tables exist)

ALTER TABLE sell.fulfillapi_orders ADD CONSTRAINT fulfillapi_orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES sell.customers(id);
ALTER TABLE sell.fulfillapi_orders ADD CONSTRAINT fulfillapi_orders_ecommerce_source_fkey FOREIGN KEY (ecommerce_source) REFERENCES sell.ecommerce_sources(source);


CREATE TABLE IF NOT EXISTS sell.fulfillapi_order_line_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    order_id UUID,
    source_order_line_item_id TEXT NULL,
    sku TEXT,
    quantity INTEGER DEFAULT 1,
    price NUMERIC NULL,
    source_metadata JSONB NULL,
    status TEXT NULL,
    customization JSONB NULL,
    revenue_share_code TEXT NULL,
    metadata JSONB NULL,
    name TEXT NULL,
    product_id UUID NULL,
    fulfillable_quantity INTEGER NULL,
    fulfilled_quantity INTEGER NULL,
    refunded_quantity INTEGER NULL,
    retail_price NUMERIC NULL,
    basic_cost_of_one NUMERIC NULL,
    revenue_share_amount NUMERIC NULL 
);

-- Add foreign keys if they don't exist (assuming referenced tables exist)

ALTER TABLE sell.fulfillapi_order_line_items ADD CONSTRAINT fulfillapi_order_line_items_fk_order_id FOREIGN KEY (order_id) REFERENCES sell.orders(id);

ALTER TABLE sell.fulfillapi_order_line_items ADD CONSTRAINT fulfillapi_order_line_items_fk_product_id FOREIGN KEY (product_id) REFERENCES sell.products(id);



DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'basic_one_cost_category_enum') THEN
        CREATE TYPE sell.basic_one_cost_category_enum AS ENUM (
            'Overhead', 'Labor', 'Materials'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'basic_one_cost_unit_enum') THEN
        CREATE TYPE sell.basic_one_cost_unit_enum AS ENUM (
            '', 'Yards', 'Pieces', 'Minutes', 'bundle'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'basic_one_cost_item_enum') THEN
        CREATE TYPE sell.basic_one_cost_item_enum AS ENUM (
            'Overhead', 'Print', 'Cut', 'Sew', 'Trim'
        );
    END IF;
END $$;

-- Create the table fufillapi_order_line_item_costs
                                
CREATE TABLE IF NOT EXISTS sell.fulfillapi_order_line_item_costs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_line_item_id UUID,
    item_enum sell.basic_one_cost_item_enum NULL,  -- Enum column
    material_name TEXT NULL,  -- Renamed and changed to TEXT -- cant FK this as material code not unique in meta.materials.key
    category sell.basic_one_cost_category_enum  NULL,
    rate NUMERIC  NULL,
    unit sell.basic_one_cost_unit_enum  NULL,
    quantity NUMERIC  NULL,
    cost NUMERIC  NULL,
    CHECK (
        (item_enum IS NOT NULL AND material_name IS NULL) OR 
        (item_enum IS NULL AND material_name IS NOT NULL)
    )  -- CHECK constraint
);

-- Add foreign key for order_line_item_id
ALTER TABLE sell.fulfillapi_order_line_item_costs ADD CONSTRAINT fk_order_line_item_id FOREIGN KEY (order_line_item_id) REFERENCES sell.fulfillapi_order_line_items(id);



