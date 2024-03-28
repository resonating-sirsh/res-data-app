# SQL Queries

INSERT_FULLFILMENT_STATUS_HISTORY = """
INSERT INTO sell.order_item_fulfillments_status_history 
(
  id, 
  order_item_fulfillment_id, 
  order_line_item_id,
  updated_at, 
  ready_to_ship_datetime, 
  shipped_datetime, 
  received_by_cust_datetime, 
  shipping_notification_sent_cust_datetime, 
  process_id, 
  metadata, 
  current_sku,
  old_sku,
  quantity,
  order_number,
  fulfilled_from_inventory,
  hand_delivered


) 
VALUES 
(%(id)s, 
%(order_item_fulfillment_id)s, 
%(order_line_item_id)s, 
%(updated_at)s, 
%(ready_to_ship_datetime)s, 
%(shipped_datetime)s, 
%(received_by_cust_datetime)s, 
%(shipping_notification_sent_cust_datetime)s, 
%(process_id)s, 
%(metadata)s::jsonb, 
%(current_sku)s, 
%(old_sku)s, 
%(quantity)s, 
%(order_number)s,
%(fulfilled_from_inventory)s,
%(hand_delivered)s);
"""


GET_SELL_ORDER_ITEM_GIVEN_SKU_ORDERNUMBER = """
SELECT 
soli.sku, soli.quantity, so.name, soli.refunded_quantity, soli.fulfilled_quantity, so.status, soli.status from sell.orders so 
INNER JOIN sell.order_line_items soli on soli.order_id = so.id
WHERE soli.sku = %s
and so.name =  %s """

GET_ORDER_ITEM_FULFILLMENT_ID_GIVEN_ONE_NUMBER = """
SELECT oif.id as oif_id,
 oo.id as oo_id, 
 oo.one_number, 
 oif.status, 
 oif.order_item_ordinal, 
 oo.make_instance, 
 oo.order_number
FROM sell.order_item_fulfillments oif
INNER JOIN make.one_orders oo
ON oif.make_order_id = oo.id
WHERE oo.one_number = %s """

UPDATE_BRIDGE_SKU_ONE_COUNTER = """
UPDATE infraestructure.bridge_sku_one_counter 
	SET fulfilled_at = %(shipped_datetime)s, 
	updated_at = CURRENT_TIMESTAMP
	WHERE 
	
	one_number = %(one_number)s 
    RETURNING id;
"""
GET_ORDER_LINE_ITEM_ID_GIVEN_ORDER_AND_SKU = """
SELECT soli.id
FROM sell.order_line_items soli
INNER JOIN sell.orders so
ON soli.order_id = so.id
WHERE so.name = %s
AND soli.SKU = %s """

GET_ORDER_LINE_ITEM_ID_GIVEN_ONE_NUMBER_AND_ORDER_NUM = """
	select soli.id from sell.order_line_items soli 
	inner join sell.orders so 
	ON so.id = soli.order_id
	where soli.sku = (select sku from make.one_orders where one_number = %s )
	and so.name = %s """


GET_TRANSACTIONS_BASE_QUERY = """
    SELECT 
    st.id as transaction_pkid,
    st.amount as order_total_amount,
    st.subscription as subscription_pkid,
    st.created_at,
    st.updated_at,
    st.type as tranaction_type,
    st.source,
    st.reference_id,
    st.currency,
    st.description,
    st.stripe_charge_id,
    st.direct_stripe_payment,
    st.is_failed_transaction,
    st.transaction_status,
    st.transaction_error_message,
    sb.airtable_brand_code as brand_code,
    ss.name as subscription_name,
    td.id as transactions_detail_pkid,
    td.make_cost,
td.price,
td."type" as transaction_details_type,
td.order_number,
td.order_date,
td.order_type ,
td.revenue_share,
td.total_amount as line_item_total_amount
    FROM sell.transactions ST
    INNER JOIN  sell.subscriptions ss on st.subscription = ss.id
    INNER JOIN sell.Brands sb on sb.id = ss.brand_id
    LEFT OUTER JOIN sell.transaction_details td on td.transaction_id = ST.id
    WHERE {0}
"""


GET_BRAND_BY_BRANDCODE = """

SELECT
b.id as brand_PKID,
b.id_uuid,
b.meta_record_id,
b.fulfill_record_id,
b.brand_code,
b.is_brand_whitelist_payment,
b.active_subscription_id,
b."name",
b.shopify_storename,
b.order_delayed_email_v2,
b.created_at_airtable,
b.shopify_store_name,
b.homepage_url,
b.sell_enabled,
b.shopify_location_id_nyc,
b.contact_email,
b.payments_revenue_share_ecom,
b.created_at_year,
b.payments_revenue_share_wholesale,
b.subdomain_name,
b.brands_create_one_url,
b.shopify_shared_secret,
b.quickbooks_manufacturing_id,
b.address,
b.is_direct_payment_default,
b.brand_success_active,
b.shopify_api_key,
b.shopify_api_password,
b.register_brand_email_address,
b.start_date,
b.end_date,
b.is_exempt_payment_setup_for_ordering_createone,
b.must_pay_before_make,
s."name" as sub_name,
s.subscription_id as stripe_subscription_id,
s.id as sub_pkid,
s.created_at as sub_created_at,
s.updated_at as sub_updated_at,
s.start_date as sub_start_date,
s.end_date as sub_end_date,
s.collection_method as sub_collection_method,
s.balance,
s.price as stripe_price_id,
s.stripe_customer_id,
s.payment_method as stripe_payment_method,
s.currency,
s.current_period_start as sub_current_period_start,
s.current_period_end as sub_current_period_end,
s.price_amount as sub_price_amount,
s.is_direct_payment_default as sub_is_direct_payment_default

FROM SELL.BRANDS b
LEFT OUTER JOIN sell.subscriptions s
ON (s.brand_id = b.id AND (s.deleted_from IS NULL OR CURRENT_DATE < s.deleted_from  ) )
WHERE b.brand_code = %s
AND (b.end_date >= CURRENT_DATE or b.end_date  is null)
AND (b.start_date <= CURRENT_DATE or b.start_date  is null)
"""
