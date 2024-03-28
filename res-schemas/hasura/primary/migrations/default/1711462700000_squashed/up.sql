

CREATE OR REPLACE FUNCTION public.fn_archive_brands()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
    must_pay_before_make,
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
    OLD.must_pay_before_make,
    NOW()
  );
 
 
   IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSIF TG_OP = 'UPDATE' THEN
    RETURN NEW;
  END IF;
END;
$function$


