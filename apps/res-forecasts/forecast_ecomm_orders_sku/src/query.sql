with 

orders as (

    select 
      split_part(style_code, ' ', 1) as body_code,
      brand_name,
      brand_code,
      '' as body_category,
      style_code,
      size,
      replace(style_code, '-', '') || ' ' || size as sku,
      body_full_code,
      color,
      color_code,
      material_code,
      base_fabric_material,
      material_type,
      material_weight_category,
      ordered_at
    from iamcurious_db.iamcurious_production_models.fact_latest_order_line_items
      where order_channel = 'Shopify'
),

styles as (
    
    select 
        style_id,
        body_code || ' ' || material_code || ' ' || color_code as style_code,
        body_code,
        body_name,
        body_version,
        body_category,
        color_code,
        color_name,
        array_distinct(split(color_type, ',')) as color_types,
        style_name_short

    from IAMCURIOUS_DB.IAMCURIOUS_production_models.FACT_latest_CREATE_STYLES
),

colors as (

    select color_code, color_types
    from styles
    qualify row_number() over (partition by color_code order by color_types is null, array_size(color_types)) = 1
),

bodies as (

  select body_code,
    body_category
  from styles qualify row_number() over (partition by body_code order by body_category is null) = 1
),

material_taxonomy as(
        SELECT
        purchasing_material_key as material_code,
        material_taxonomy_name,
        material_taxonomy_category,
        material_taxonomy_parent_name,
        mt.fabric_type,
        dominant_content,
        weight_category,
        stretch_type
    FROM IAMCURIOUS_DB.IAMCURIOUS_PRODUCTION_MODELS.fact_latest_purchasing_materials m
    LEFT JOIN IAMCURIOUS_DB.IAMCURIOUS_PRODUCTION_MODELS.fact_latest_purchasing_material_taxonomy mt
        ON m.material_taxonomy_parent_id = mt.material_taxonomy_id
),

nested_length as (
  select distinct
    one_sku,
    --prepared_pieces_nested_length_yards
    last_value(prepared_pieces_nested_length_yards ignore nulls) over (partition by one_sku order by created_at) as prepared_pieces_nested_length_yards
  from iamcurious_db.iamcurious_production_models.fact_latest_print_assets
    --qualify row_number() over (partition by one_sku order by created_at desc) = 1
),

joined as (

select
  ordered_at :: date as order_date,
  orders.brand_name,
  orders.brand_code,
  orders.sku,
  styles.style_id,
  orders.style_code,
  orders.size,
  orders.material_code,
  bodies.body_category,
  orders.color_code,
  styles.color_types,
  mt.material_taxonomy_name,
  mt.material_taxonomy_parent_name,
  mt.fabric_type = 'Knit' as is_knit_material,
  mt.dominant_content,
  mt.weight_category,
  case when mt.weight_category = 'Light-Weight' then 0
    when mt.weight_category = 'Mid-Weight' then 1
    when mt.weight_category = 'Heavy-Weight' then 2
  end as weight_category_num,
  mt.stretch_type = 'Stretch' as is_stretch_material,
  prepared_pieces_nested_length_yards,
  count(ordered_at) as line_item_count
from orders
left join styles on orders.style_code = styles.style_code
left join colors on orders.color_code = colors.color_code
left join bodies on styles.body_code = bodies.body_code
left join material_taxonomy mt on orders.material_code = mt.material_code
left join nested_length on orders.sku = nested_length.one_sku
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
order by 1,5,2,3,4,6,7,8,9,10,11,12,13
),

trailing_total_week as (

  select
    sku,
    sum(line_item_count) as trailing_total_orders_week
  from joined
  where order_date >= dateadd(day, -7, current_timestamp())
  group by 1
),

trailing_total_month as (

  select
    sku,
    sum(line_item_count) as trailing_total_orders_month
  from joined
  where order_date >= dateadd(day, -30, current_timestamp())
  group by 1
),

trailing_total_quarter as (

  select
    sku,
    sum(line_item_count) as trailing_total_orders_quarter
  from joined
  where order_date >= dateadd(day, -90, current_timestamp())
  group by 1
),

with_trailing_totals as (

  select
    brand_code,
    brand_name,
    style_code,
    sku,
    material_code,
    color_code,
    body_category,
    is_knit_material,
    dominant_content,
    weight_category,
    is_stretch_material,
    prepared_pieces_nested_length_yards,
    coalesce(trailing_total_orders_week, 0) as trailing_total_orders_week,
    coalesce(trailing_total_orders_month, 0) as trailing_total_orders_month,
    coalesce(trailing_total_orders_quarter, 0) as trailing_total_orders_quarter,
    round((coalesce(trailing_total_orders_week, 0) + coalesce(trailing_total_orders_month, 0)) / 2) as recommended_order_quantity,
    max(order_date) as last_ordered_on
    
  from joined
  left join trailing_total_week using (sku)
  left join trailing_total_month using (sku)
  left join trailing_total_quarter using (sku)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),

ranked as (

  select
    *
  from  with_trailing_totals
  where recommended_order_quantity > 0
)

select
  material_code,
  brand_code,
  brand_name,
  sku,
  prepared_pieces_nested_length_yards,
  recommended_order_quantity
from ranked
