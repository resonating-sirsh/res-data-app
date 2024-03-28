VIEW_TEMPLATE = """
view: {view_name} {{
  derived_table: {{
        sql: {table_sql};;
  }}
  {dimensions}
  {measures}
  ### CUSTOM CODE ###
  {custom_code}
}}
"""

DIMENSION_TEMPLATE = """
  dimension: {dimension_name} {{
    type: {dimension_type}
    sql: ${{TABLE}}.{column_name} ;;
  }}
"""

TIME_DIMENSION_TEMPLATE = """
  dimension_group: {dimension_name} {{
    type: time
    timeframes: [
      date,
      week,
      week_of_year,
      month,
      month_num,
      year
    ]
    sql: ${{TABLE}}.{column_name} ;;
  }}
"""

MEASURE_COUNT_TEMPLATE = """
  measure: count {
    type: count
  }
"""


MEASURE_TEMPLATE = """
  measure: {measure_name} {{
    type: {measure_type}
    sql: ${{TABLE}}.{column_name};;
  }}
"""

MODEL_TEMPLATE = """
connection: "snow_cnx"
include: "/{base_views_folder}/*.view.lkml"
{explores}
"""

EXPLORE_TEMPLATE = """
explore: {explore_name} {{
  label: "{label}"
  description: "{description}"
  {joins}
}}
"""

EXPLORE_JOIN_TEMPLATE = """
  join: {join_name} {{
    view_label: "{view_label}"
    sql_on: {sql_on};;
    type: {type}
    relationship: {relationship}
  }}
"""
