DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_tables
    WHERE schemaname = 'sell'
    AND tablename = 'order_item_fulfillments_status_history'
  )
  THEN
    CREATE TABLE sell.order_item_fulfillments_status_history (
      id UUID PRIMARY KEY,
      order_item_fulfillment_id UUID NULL REFERENCES sell.order_item_fulfillments(id), -- FK reference here
      order_line_item_id UUID NULL REFERENCES sell.order_line_items(id),
	  updated_at TIMESTAMP WITH TIME ZONE,
      ready_to_ship_datetime TIMESTAMP WITH TIME ZONE,
      shipped_datetime TIMESTAMP WITH TIME ZONE,
      received_by_cust_datetime TIMESTAMP WITH TIME ZONE,
      shipping_notification_sent_cust_datetime TIMESTAMP WITH TIME ZONE,
      current_sku VARCHAR(50),
	  old_sku VARCHAR(50),
	  quantity integer,
	  order_number varchar(20),
	  fulfilled_from_inventory bool,
	  hand_delivered bool,
	  process_id VARCHAR(50),
      metadata JSONB
    );
  END IF;
END $$;