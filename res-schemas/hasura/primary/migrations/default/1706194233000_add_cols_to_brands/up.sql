DO $$
BEGIN
    -- Add or modify columns in 'sell.brands'
    ALTER TABLE sell.brands
    ADD COLUMN IF NOT EXISTS id_uuid UUID,
    ADD COLUMN IF NOT EXISTS meta_record_id TEXT,
    ADD COLUMN IF NOT EXISTS fulfill_record_id TEXT,
    ADD COLUMN IF NOT EXISTS brand_code TEXT,
    ADD COLUMN IF NOT EXISTS is_brand_whitelist_payment BOOLEAN,
    ADD COLUMN IF NOT EXISTS active_subscription_id INTEGER,
    ADD COLUMN IF NOT EXISTS name TEXT,
    ADD COLUMN IF NOT EXISTS shopify_storename TEXT,
    ADD COLUMN IF NOT EXISTS order_delayed_email_v2 BOOLEAN,
    ADD COLUMN IF NOT EXISTS created_at_airtable TIMESTAMP,
    ADD COLUMN IF NOT EXISTS shopify_store_name TEXT,
    ADD COLUMN IF NOT EXISTS homepage_url TEXT,
    ADD COLUMN IF NOT EXISTS sell_enabled BOOLEAN,
    ADD COLUMN IF NOT EXISTS shopify_location_id_nyc TEXT,
    ADD COLUMN IF NOT EXISTS contact_email TEXT,
    ADD COLUMN IF NOT EXISTS payments_revenue_share_ecom NUMERIC,
    ADD COLUMN IF NOT EXISTS created_at_year INTEGER,
    ADD COLUMN IF NOT EXISTS payments_revenue_share_wholesale NUMERIC,
    ADD COLUMN IF NOT EXISTS subdomain_name TEXT,
    ADD COLUMN IF NOT EXISTS brands_create_one_url TEXT,
    ADD COLUMN IF NOT EXISTS shopify_shared_secret TEXT,
    ADD COLUMN IF NOT EXISTS quickbooks_manufacturing_id TEXT,
    ADD COLUMN IF NOT EXISTS address TEXT,
    ADD COLUMN IF NOT EXISTS is_direct_payment_default BOOLEAN,
    ADD COLUMN IF NOT EXISTS brand_success_active BOOLEAN,
    ADD COLUMN IF NOT EXISTS shopify_api_key TEXT,
    ADD COLUMN IF NOT EXISTS shopify_api_password TEXT,
    ADD COLUMN IF NOT EXISTS register_brand_email_address TEXT,
    ADD COLUMN IF NOT EXISTS start_date TIMESTAMP,
    ADD COLUMN IF NOT EXISTS end_date TIMESTAMP,
    ADD COLUMN IF NOT EXISTS is_exempt_payment_setup_for_ordering_createone BOOLEAN;
     
END $$;
