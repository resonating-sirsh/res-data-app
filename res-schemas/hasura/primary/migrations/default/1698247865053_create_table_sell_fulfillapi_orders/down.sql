DROP TABLE  IF EXISTS sell.fulfillapi_order_line_item_costs;
--DROP TABLE  IF EXISTS sell.fufillapi_order_line_item_costs;
DROP TABLE IF EXISTS sell.fulfillapi_order_line_items;
DROP TABLE IF EXISTS sell.fulfillapi_orders;
DROP TABLE IF EXISTS sell.fulfillapi_order_line_items;
DROP TABLE IF EXISTS sell.fulfillapi_orders;


-- Drop the ENUM types
DROP TYPE IF EXISTS sell.basic_one_cost_item_enum;
DROP TYPE IF EXISTS sell.basic_one_cost_category_enum;
DROP TYPE IF EXISTS sell.basic_one_cost_unit_enum;