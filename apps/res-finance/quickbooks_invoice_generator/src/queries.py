SNOWFLAKE_RES_MANUFACTURING_ORDER_LINE_ITEMS = """
        with
        line_items as (
            select
                order_line_item_id,   
                order_id,   
                style_id,            
                brand_code,        
                line_item_price_usd,
                fulfillment_shipping_cost_usd,  
                line_item_sku,
                order_number, 
                sales_channel,    
                size,      
                coalesce(archived_at, deleted_at) is null as is_in_airtable,
                shipping_tracking_number_line_item is not null as has_tracking_number,
                is_found_in_inventory and lower(warehouse_location) = 'nyc' as is_fulfilled_from_nyc,
                created_at,
                fulfillment_completed_at
            from fact_latest_order_line_items
            where
                fulfillment_completed_at::date >= '{start_date}'
                and fulfillment_completed_at::date <= '{end_date}'
        ),

        earliest_costs as (
            select
                historical_style_size_cost_id,
                style_id,
                style_code,
                size_code,
                body_piece_count,
                material_quantity_yards,
                piece_material_codes,    
                piece_material_quantities_yards,
                print_quantity_yards,
                sewing_time_minutes,    
                surrogate_body_piece_count,
                surrogate_material_quantity_yards,
                surrogate_piece_material_codes,
                surrogate_piece_material_quantities_yards,
                surrogate_print_quantity_yards,
                surrogate_sewing_time_minutes,  
                piece_material_cost_rates_usd,
                cut_node_cost_rate_usd,
                print_node_cost_rate_usd,
                sew_node_cost_rate_usd, 
                cut_node_cost_usd,
                material_cost_usd,
                piece_material_costs_usd,
                print_cost_usd,
                sew_node_cost_usd,
                surrogate_cut_node_cost_usd,
                surrogate_material_cost_usd,
                surrogate_piece_material_costs_usd,
                surrogate_print_cost_usd,
                surrogate_sew_node_cost_usd,
                surrogate_trim_cost_usd,
                trim_cost_usd,     
                valid_from,
                valid_to
            from fact_historical_style_size_costs
            qualify row_number() over (
                partition by style_id, size_code
                order by valid_from asc
            ) = 1
        )
    
        select
            coalesce(
                costs.historical_style_size_cost_id,
                costs_at_ful.historical_style_size_cost_id,
                earliest_costs.historical_style_size_cost_id
            ) as historical_style_size_cost_id,
            line_items.order_id,   
            line_items.style_id,            
            line_items.brand_code,        
            line_items.line_item_sku as sku,
            line_items.order_number, 
            line_items.is_in_airtable, 
            line_items.has_tracking_number, 
            line_items.is_fulfilled_from_nyc,
            week(line_items.created_at) as created_at_week,
            line_items.sales_channel as channel,            
            coalesce(
                costs.piece_material_codes, 
                costs.surrogate_piece_material_codes, 
                costs_at_ful.piece_material_codes, 
                costs_at_ful.surrogate_piece_material_codes,
                earliest_costs.piece_material_codes, 
                earliest_costs.surrogate_piece_material_codes                
            ) as material_codes, 
            coalesce(
                costs.print_node_cost_rate_usd, 
                costs_at_ful.print_node_cost_rate_usd,
                earliest_costs.print_node_cost_rate_usd
            ) as print_rate,
            coalesce(
                costs.print_quantity_yards, 
                costs.surrogate_print_quantity_yards, 
                costs_at_ful.print_quantity_yards, 
                costs_at_ful.surrogate_print_quantity_yards,
                earliest_costs.print_quantity_yards, 
                earliest_costs.surrogate_print_quantity_yards                
            ) as print_quantity,
            coalesce(
                costs.cut_node_cost_rate_usd,  
                costs_at_ful.cut_node_cost_rate_usd,
                earliest_costs.cut_node_cost_rate_usd
            ) as cut_rate,
            coalesce(
                costs.body_piece_count, 
                costs.surrogate_body_piece_count, 
                costs_at_ful.body_piece_count, 
                costs_at_ful.surrogate_body_piece_count,
                earliest_costs.body_piece_count, 
                earliest_costs.surrogate_body_piece_count
            ) as cut_quantity,
            coalesce(
                costs.sew_node_cost_rate_usd,
                costs_at_ful.sew_node_cost_rate_usd,
                earliest_costs.sew_node_cost_rate_usd
            ) as sew_rate,
            coalesce(
                costs.sewing_time_minutes, 
                costs.surrogate_sewing_time_minutes, 
                costs_at_ful.sewing_time_minutes, 
                costs_at_ful.surrogate_sewing_time_minutes,
                earliest_costs.sewing_time_minutes, 
                earliest_costs.surrogate_sewing_time_minutes
            ) as sew_quantity,
            coalesce(
                costs.piece_material_cost_rates_usd, 
                costs_at_ful.piece_material_cost_rates_usd,
                earliest_costs.piece_material_cost_rates_usd
            ) as material_rates,
            coalesce(
                costs.piece_material_quantities_yards, 
                costs.surrogate_piece_material_quantities_yards, 
                costs_at_ful.piece_material_quantities_yards, 
                costs_at_ful.surrogate_piece_material_quantities_yards,
                earliest_costs.piece_material_quantities_yards, 
                earliest_costs.surrogate_piece_material_quantities_yards
            ) as material_quantities,
            coalesce(
                costs.trim_cost_usd, 
                costs.surrogate_trim_cost_usd, 
                costs_at_ful.trim_cost_usd,
                costs_at_ful.surrogate_trim_cost_usd,
                earliest_costs.trim_cost_usd,
                earliest_costs.surrogate_trim_cost_usd
            ) as trim_cost,
            round(
                (
                    coalesce(
                        costs.cut_node_cost_usd, 
                        costs.surrogate_cut_node_cost_usd, 
                        costs_at_ful.cut_node_cost_usd, 
                        costs_at_ful.surrogate_cut_node_cost_usd, 
                        earliest_costs.cut_node_cost_usd, 
                        earliest_costs.surrogate_cut_node_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.material_cost_usd, 
                        costs.surrogate_material_cost_usd, 
                        costs_at_ful.material_cost_usd, 
                        costs_at_ful.surrogate_material_cost_usd, 
                        earliest_costs.material_cost_usd, 
                        earliest_costs.surrogate_material_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.print_cost_usd, 
                        costs.surrogate_print_cost_usd, 
                        costs_at_ful.print_cost_usd, 
                        costs_at_ful.surrogate_print_cost_usd, 
                        earliest_costs.print_cost_usd, 
                        earliest_costs.surrogate_print_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.sew_node_cost_usd, 
                        costs.surrogate_sew_node_cost_usd, 
                        costs_at_ful.sew_node_cost_usd, 
                        costs_at_ful.surrogate_sew_node_cost_usd, 
                        earliest_costs.sew_node_cost_usd, 
                        earliest_costs.surrogate_sew_node_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.trim_cost_usd, 
                        costs.surrogate_trim_cost_usd, 
                        costs_at_ful.trim_cost_usd, 
                        costs_at_ful.surrogate_trim_cost_usd, 
                        earliest_costs.trim_cost_usd, 
                        earliest_costs.surrogate_trim_cost_usd, 
                        0
                    )             
                ) * 1.2,
                2
            ) as total_cost,
            line_items.created_at::date as created_date,
            line_items.fulfillment_completed_at::date as fulfillment_completed_date,
            case
                when nullif(line_items.line_item_price_usd, 0) is not null 
                    then line_items.line_item_price_usd
                when lower(line_items.sales_channel) in (
                    'ecom',
                    'e-commerce'
                )
                    then mongo_prices.price_usd
                when lower(line_items.sales_channel) = 'wholesale'
                    then mongo_prices.wholesale_price_usd
            end as line_item_price,
            case
                when lower(line_items.sales_channel) in (
                    'ecom',
                    'e-commerce'
                )
                    then round(
                        brands.ecom_revenue_share_percent * line_item_price, 
                        2
                    )
                when lower(line_items.sales_channel) = 'wholesale'
                    then round(
                        brands.wholesale_revenue_share_percent * line_item_price, 
                        2
                    )
            end as revenue_share,
            line_items.fulfillment_shipping_cost_usd as fulfillment_shipping_cost,
            round(coalesce(total_cost, 0) + coalesce(revenue_share, 0) + coalesce(fulfillment_shipping_cost, 0), 2) as total_due,           
            count(*) as count,
            listagg(line_items.order_line_item_id, ', ') as order_line_item_ids
        from line_items
        left join fact_historical_mongodb_style_prices as mongo_prices
            on
                line_items.style_id = mongo_prices.style_id
                and line_items.created_at >= mongo_prices.valid_from
                and line_items.created_at < coalesce(
                    mongo_prices.valid_to, 
                    current_timestamp()
                )
        left join br_latest_brand_code_ids as br_brand
            on
                line_items.brand_code = br_brand.brand_code
        /* 
            Uses latest brands because many brands historically lacked full data 
            in Airtable + information like revenue share and address still needs 
            to be the latest agreed even for past charges 
        */
        left join fact_latest_brands as brands
            on br_brand.brand_id = brands.brand_id
        left join fact_historical_style_size_costs as costs
            on 
                line_items.style_id = costs.style_id
                and line_items.size = costs.size_code
                and line_items.created_at >= costs.valid_from
                and line_items.created_at < coalesce(
                    costs.valid_to,
                    current_timestamp()
                )
        /* 
            Sometimes costs are missing at the time of order. In these cases the cost
            at time of fulfillment or the earliest observed cost is used
        */
        left join fact_historical_style_size_costs as costs_at_ful
            on 
                line_items.style_id = costs_at_ful.style_id
                and line_items.size = costs_at_ful.size_code
                and line_items.fulfillment_completed_at >= costs_at_ful.valid_from
                and line_items.fulfillment_completed_at < coalesce(
                    costs_at_ful.valid_to,
                    current_timestamp()
                ) 
        left join earliest_costs
            on 
                line_items.style_id = earliest_costs.style_id
                and line_items.size = earliest_costs.size_code                
        group by all
        order by line_items.brand_code asc, created_date asc, line_items.order_id asc
    """

SNOWFLAKE_RES_COMPANIES_ORDER_LINE_ITEMS = """
        with
        line_items as (
            select
                order_line_item_id,
                order_id,   
                style_id,            
                brand_code,        
                line_item_price_usd,
                fulfillment_shipping_cost_usd,  
                line_item_sku,
                order_number, 
                sales_channel,    
                size,      
                coalesce(archived_at, deleted_at) is null as is_in_airtable,      
                created_at,
                fulfillment_completed_at,
                first_value(fulfillment_completed_at::date) over (
                    partition by order_id 
                    order by fulfillment_completed_at nulls last
                ) as order_first_fulfillment_at                
            from fact_latest_order_line_items
            qualify 
                /*
                    This qualify statement includes line items if any line item
                    within their order has been fulfilled within the timeframe
                    specified
                */
                order_first_fulfillment_at::date >= '{start_date}'
                and order_first_fulfillment_at::date <= '{end_date}' 
        ),

        earliest_costs as (
            select
                historical_style_size_cost_id,
                style_id,
                style_code,
                size_code,
                body_piece_count,
                material_quantity_yards,
                piece_material_codes,    
                piece_material_quantities_yards,
                print_quantity_yards,
                sewing_time_minutes,    
                surrogate_body_piece_count,
                surrogate_material_quantity_yards,
                surrogate_piece_material_codes,
                surrogate_piece_material_quantities_yards,
                surrogate_print_quantity_yards,
                surrogate_sewing_time_minutes,  
                piece_material_cost_rates_usd,
                cut_node_cost_rate_usd,
                print_node_cost_rate_usd,
                sew_node_cost_rate_usd, 
                cut_node_cost_usd,
                material_cost_usd,
                piece_material_costs_usd,
                print_cost_usd,
                sew_node_cost_usd,
                surrogate_cut_node_cost_usd,
                surrogate_material_cost_usd,
                surrogate_piece_material_costs_usd,
                surrogate_print_cost_usd,
                surrogate_sew_node_cost_usd,
                surrogate_trim_cost_usd,
                trim_cost_usd,     
                valid_from,
                valid_to
            from fact_historical_style_size_costs
            qualify row_number() over (
                partition by style_id, size_code
                order by valid_from asc
            ) = 1
        )
    
        select
            coalesce(
                costs.historical_style_size_cost_id,
                costs_at_ful.historical_style_size_cost_id,
                earliest_costs.historical_style_size_cost_id
            ) as historical_style_size_cost_id,        
            line_items.order_id,   
            line_items.style_id,            
            line_items.brand_code,        
            line_items.line_item_sku as sku,
            line_items.order_number, 
            line_items.is_in_airtable, 
            week(line_items.created_at) as created_at_week,
            line_items.sales_channel as channel,
            coalesce(
                costs.piece_material_codes, 
                costs.surrogate_piece_material_codes, 
                costs_at_ful.piece_material_codes, 
                costs_at_ful.surrogate_piece_material_codes,
                earliest_costs.piece_material_codes, 
                earliest_costs.surrogate_piece_material_codes                
            ) as material_codes, 
            coalesce(
                costs.print_node_cost_rate_usd, 
                costs_at_ful.print_node_cost_rate_usd,
                earliest_costs.print_node_cost_rate_usd
            ) as print_rate,
            coalesce(
                costs.print_quantity_yards, 
                costs.surrogate_print_quantity_yards, 
                costs_at_ful.print_quantity_yards, 
                costs_at_ful.surrogate_print_quantity_yards,
                earliest_costs.print_quantity_yards, 
                earliest_costs.surrogate_print_quantity_yards                
            ) as print_quantity,
            coalesce(
                costs.cut_node_cost_rate_usd,  
                costs_at_ful.cut_node_cost_rate_usd,
                earliest_costs.cut_node_cost_rate_usd
            ) as cut_rate,
            coalesce(
                costs.body_piece_count, 
                costs.surrogate_body_piece_count, 
                costs_at_ful.body_piece_count, 
                costs_at_ful.surrogate_body_piece_count,
                earliest_costs.body_piece_count, 
                earliest_costs.surrogate_body_piece_count
            ) as cut_quantity,
            coalesce(
                costs.sew_node_cost_rate_usd,
                costs_at_ful.sew_node_cost_rate_usd,
                earliest_costs.sew_node_cost_rate_usd
            ) as sew_rate,
            coalesce(
                costs.sewing_time_minutes, 
                costs.surrogate_sewing_time_minutes, 
                costs_at_ful.sewing_time_minutes, 
                costs_at_ful.surrogate_sewing_time_minutes,
                earliest_costs.sewing_time_minutes, 
                earliest_costs.surrogate_sewing_time_minutes
            ) as sew_quantity,
            coalesce(
                costs.piece_material_cost_rates_usd, 
                costs_at_ful.piece_material_cost_rates_usd,
                earliest_costs.piece_material_cost_rates_usd
            ) as material_rates,
            coalesce(
                costs.piece_material_quantities_yards, 
                costs.surrogate_piece_material_quantities_yards, 
                costs_at_ful.piece_material_quantities_yards, 
                costs_at_ful.surrogate_piece_material_quantities_yards,
                earliest_costs.piece_material_quantities_yards, 
                earliest_costs.surrogate_piece_material_quantities_yards
            ) as material_quantities,
            coalesce(
                costs.trim_cost_usd, 
                costs.surrogate_trim_cost_usd, 
                costs_at_ful.trim_cost_usd,
                costs_at_ful.surrogate_trim_cost_usd,
                earliest_costs.trim_cost_usd,
                earliest_costs.surrogate_trim_cost_usd
            ) as trim_cost,
            round(
                (
                    coalesce(
                        costs.cut_node_cost_usd, 
                        costs.surrogate_cut_node_cost_usd, 
                        costs_at_ful.cut_node_cost_usd, 
                        costs_at_ful.surrogate_cut_node_cost_usd, 
                        earliest_costs.cut_node_cost_usd, 
                        earliest_costs.surrogate_cut_node_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.material_cost_usd, 
                        costs.surrogate_material_cost_usd, 
                        costs_at_ful.material_cost_usd, 
                        costs_at_ful.surrogate_material_cost_usd, 
                        earliest_costs.material_cost_usd, 
                        earliest_costs.surrogate_material_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.print_cost_usd, 
                        costs.surrogate_print_cost_usd, 
                        costs_at_ful.print_cost_usd, 
                        costs_at_ful.surrogate_print_cost_usd, 
                        earliest_costs.print_cost_usd, 
                        earliest_costs.surrogate_print_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.sew_node_cost_usd, 
                        costs.surrogate_sew_node_cost_usd, 
                        costs_at_ful.sew_node_cost_usd, 
                        costs_at_ful.surrogate_sew_node_cost_usd, 
                        earliest_costs.sew_node_cost_usd, 
                        earliest_costs.surrogate_sew_node_cost_usd, 
                        0
                    )
                    + coalesce(
                        costs.trim_cost_usd, 
                        costs.surrogate_trim_cost_usd, 
                        costs_at_ful.trim_cost_usd, 
                        costs_at_ful.surrogate_trim_cost_usd, 
                        earliest_costs.trim_cost_usd, 
                        earliest_costs.surrogate_trim_cost_usd, 
                        0
                    )             
                ) * 1.2,
                2
            ) as total_cost,
            line_items.created_at::date as created_date,
            line_items.fulfillment_completed_at::date as fulfillment_completed_date,
            case
                when nullif(line_items.line_item_price_usd, 0) is not null 
                    then line_items.line_item_price_usd
                when lower(line_items.sales_channel) in (
                    'ecom',
                    'e-commerce'
                )
                    then mongo_prices.price_usd
                when lower(line_items.sales_channel) = 'wholesale'
                    then mongo_prices.wholesale_price_usd
            end as line_item_price,
            case
                when lower(line_items.sales_channel) in (
                    'ecom',
                    'e-commerce'
                )
                    then round(
                        brands.ecom_revenue_share_percent * line_item_price, 
                        2
                    )
                when lower(line_items.sales_channel) = 'wholesale'
                    then round(
                        brands.wholesale_revenue_share_percent * line_item_price, 
                        2
                    )
            end as revenue_share,
            line_items.fulfillment_shipping_cost_usd as fulfillment_shipping_cost,
            round(coalesce(total_cost, 0) + coalesce(revenue_share, 0) + coalesce(fulfillment_shipping_cost, 0), 2) as total_due,
            count(*) as count,
            listagg(line_items.order_line_item_id, ', ') as order_line_item_ids
        from line_items
        left join fact_historical_mongodb_style_prices as mongo_prices
            on
                line_items.style_id = mongo_prices.style_id
                and line_items.created_at >= mongo_prices.valid_from
                and line_items.created_at < coalesce(
                    mongo_prices.valid_to, 
                    current_timestamp()
                )
        left join br_latest_brand_code_ids as br_brand
            on
                line_items.brand_code = br_brand.brand_code
        /* 
            Uses latest brands because many brands historically lacked full data 
            in Airtable + information like revenue share and address still needs 
            to be the latest agreed even for past charges 
        */
        left join fact_latest_brands as brands
            on br_brand.brand_id = brands.brand_id
        left join fact_historical_style_size_costs as costs
            on 
                line_items.style_id = costs.style_id
                and line_items.size = costs.size_code
                and line_items.created_at >= costs.valid_from
                and line_items.created_at < coalesce(
                    costs.valid_to,
                    current_timestamp()
                )
        /* 
            Sometimes costs are missing at the time of order. In these cases the cost
            at time of fulfillment or the earliest observed cost is used
        */
        left join fact_historical_style_size_costs as costs_at_ful
            on 
                line_items.style_id = costs_at_ful.style_id
                and line_items.size = costs_at_ful.size_code
                and line_items.order_first_fulfillment_at >= costs_at_ful.valid_from
                and line_items.order_first_fulfillment_at < coalesce(
                    costs_at_ful.valid_to,
                    current_timestamp()
                ) 
        left join earliest_costs
            on 
                line_items.style_id = earliest_costs.style_id
                and line_items.size = earliest_costs.size_code                  
        group by all
        order by line_items.brand_code asc, created_date asc, line_items.order_id asc
    """

SNOWFLAKE_BRANDS = """
        with brands as (        
            select
                brand_id,
                brand_code,
                brand_name,
                quickbooks_companies_id,
                quickbooks_manufacturing_id,
                deleted_at,
                updated_at
            from fact_latest_brands
            where (
                created_at::date <= '{end_date}'
                and brand_code is not null
                and brand_code != 'TT'
                and not contains(lower(brand_name), 'test')
            )
        )

        select
            brand_id,
            brand_code,
            brand_name,
            quickbooks_companies_id,
            quickbooks_manufacturing_id,
            updated_at
        from brands
        qualify row_number() over (
            partition by brand_code
            order by (deleted_at is null) desc, updated_at desc
        ) = 1        
    """
