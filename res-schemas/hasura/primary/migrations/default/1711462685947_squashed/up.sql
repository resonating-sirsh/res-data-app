
CREATE OR REPLACE FUNCTION public.fn_archive_subscriptions()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
  
   IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSIF TG_OP = 'UPDATE' THEN
    RETURN NEW;
  END IF;
  
  END;
    $function$

