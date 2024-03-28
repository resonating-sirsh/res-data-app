-- Up migration

-- Create archive tables without unique constraints on 'id'
CREATE TABLE IF NOT EXISTS sell.brands_archive (
    archive_pkid SERIAL PRIMARY KEY,
    id INT NOT NULL,
    airtable_brand_id TEXT NOT NULL,
    airtable_brand_code TEXT NOT NULL,
    id_uuid UUID,
    meta_record_id TEXT,
    fulfill_record_id TEXT,
    brand_code TEXT,
    is_brand_whitelist_payment BOOLEAN,
    active_subscription_id INT,
    name TEXT,
    shopify_storename TEXT,
    order_delayed_email_v2 BOOLEAN,
    created_at_airtable TIMESTAMP,
    shopify_store_name TEXT,
    homepage_url TEXT,
    sell_enabled BOOLEAN,
    shopify_location_id_nyc TEXT,
    contact_email TEXT,
    payments_revenue_share_ecom NUMERIC,
    created_at_year INT,
    payments_revenue_share_wholesale NUMERIC,
    subdomain_name TEXT,
    brands_create_one_url TEXT,
    shopify_shared_secret TEXT,
    quickbooks_manufacturing_id TEXT,
    address TEXT,
    is_direct_payment_default BOOLEAN,
    brand_success_active BOOLEAN,
    shopify_api_key TEXT,
    shopify_api_password TEXT,
    register_brand_email_address TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_exempt_payment_setup_for_ordering_createone BOOLEAN,
    modified_by TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    archive_updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sell.subscriptions_archive (
    archive_pkid SERIAL PRIMARY KEY,
    name TEXT,
    subscription_id TEXT,
    id INT NOT NULL,
    deleted_status TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    collection_method TEXT,
    balance INT DEFAULT 0,
    price TEXT,
    stripe_customer_id TEXT,
    payment_method TEXT,
    currency TEXT,
    current_period_start TIMESTAMPTZ,
    current_period_end TIMESTAMPTZ,
    price_amount INT,
    brand_id INT,
    is_direct_payment_default BOOLEAN,
    deleted_from DATE,
    modified_by TEXT,
    archive_updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Add modified_by, created_at, and updated_at to the original tables
ALTER TABLE sell.brands ADD COLUMN IF NOT EXISTS modified_by TEXT;
ALTER TABLE sell.brands ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE sell.brands ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE sell.subscriptions ADD COLUMN IF NOT EXISTS modified_by TEXT;

-- Create trigger function for brands
CREATE OR REPLACE FUNCTION fn_archive_subscriptions() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO sell.subscriptions_archive (
    id,
    name,
    subscription_id,
    deleted_status,
    created_at,
    updated_at,
    start_date,
    end_date,
    collection_method,
    balance,
    price,
    stripe_customer_id,
    payment_method,
    currency,
    current_period_start,
    current_period_end,
    price_amount,
    brand_id,
    is_direct_payment_default,
    deleted_from,
    modified_by,
    archive_updated_at
    ) VALUES (
    OLD.id,
    OLD.name,
    OLD.subscription_id,
    OLD.deleted_status,
    OLD.created_at,
    OLD.updated_at,
    OLD.start_date,
    OLD.end_date,
    OLD.collection_method,
    OLD.balance,
    OLD.price,
    OLD.stripe_customer_id,
    OLD.payment_method,
    OLD.currency,
    OLD.current_period_start,
    OLD.current_period_end,
    OLD.price_amount,
    OLD.brand_id,
    OLD.is_direct_payment_default,
    OLD.deleted_from,
    OLD.modified_by,
    NOW()
    );
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;



-- Create trigger function for subscriptions
-- Trigger function for archiving brands
CREATE OR REPLACE FUNCTION fn_archive_brands() RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO sell.brands_archive (
    id,
    airtable_brand_id,
    airtable_brand_code,
    id_uuid,
    meta_record_id,
    fulfill_record_id,
    brand_code,
    is_brand_whitelist_payment,
    active_subscription_id,
    name,
    shopify_storename,
    order_delayed_email_v2,
    created_at_airtable,
    shopify_store_name,
    homepage_url,
    sell_enabled,
    shopify_location_id_nyc,
    contact_email,
    payments_revenue_share_ecom,
    created_at_year,
    payments_revenue_share_wholesale,
    subdomain_name,
    brands_create_one_url,
    shopify_shared_secret,
    quickbooks_manufacturing_id,
    address,
    is_direct_payment_default,
    brand_success_active,
    shopify_api_key,
    shopify_api_password,
    register_brand_email_address,
    start_date,
    end_date,
    is_exempt_payment_setup_for_ordering_createone,
    modified_by,
    created_at,
    updated_at,
    archive_updated_at
  ) VALUES (
    OLD.id,
    OLD.airtable_brand_id,
    OLD.airtable_brand_code,
    OLD.id_uuid,
    OLD.meta_record_id,
    OLD.fulfill_record_id,
    OLD.brand_code,
    OLD.is_brand_whitelist_payment,
    OLD.active_subscription_id,
    OLD.name,
    OLD.shopify_storename,
    OLD.order_delayed_email_v2,
    OLD.created_at_airtable,
    OLD.shopify_store_name,
    OLD.homepage_url,
    OLD.sell_enabled,
    OLD.shopify_location_id_nyc,
    OLD.contact_email,
    OLD.payments_revenue_share_ecom,
    OLD.created_at_year,
    OLD.payments_revenue_share_wholesale,
    OLD.subdomain_name,
    OLD.brands_create_one_url,
    OLD.shopify_shared_secret,
    OLD.quickbooks_manufacturing_id,
    OLD.address,
    OLD.is_direct_payment_default,
    OLD.brand_success_active,
    OLD.shopify_api_key,
    OLD.shopify_api_password,
    OLD.register_brand_email_address,
    OLD.start_date,
    OLD.end_date,
    OLD.is_exempt_payment_setup_for_ordering_createone,
    OLD.modified_by,
    OLD.created_at,
    OLD.updated_at,
    NOW()
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function


DROP TRIGGER IF EXISTS trg_archive_brands ON sell.brands;
DROP TRIGGER IF EXISTS trg_archive_subscriptions ON sell.subscriptions;

-- Apply triggers to original tables for archiving
CREATE TRIGGER trg_archive_brands BEFORE UPDATE OR DELETE ON sell.brands
    FOR EACH ROW EXECUTE FUNCTION fn_archive_brands();

CREATE TRIGGER trg_archive_subscriptions BEFORE UPDATE OR DELETE ON sell.subscriptions
    FOR EACH ROW EXECUTE FUNCTION fn_archive_subscriptions();

