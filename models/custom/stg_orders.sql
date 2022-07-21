

  create  table "ETL_source".public."raw_orders__dbt_tmp"
  as (
    
with __dbt__cte__raw_orders_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "ETL_source".public._airbyte_raw_raw_orders
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'status') as status,
    jsonb_extract_path_text(_airbyte_data, 'user_id') as user_id,
    jsonb_extract_path_text(_airbyte_data, 'order_date') as order_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "ETL_source".public._airbyte_raw_raw_orders as table_alias
-- raw_orders
where 1 = 1
),  __dbt__cte__raw_orders_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__raw_orders_ab1
select
    cast("id" as 
    float
) as "id",
    cast(status as text) as status,
    cast(user_id as 
    float
) as user_id,
    cast(nullif(order_date, '') as 
    date
) as order_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__raw_orders_ab1
-- raw_orders
where 1 = 1
),  __dbt__cte__raw_orders_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__raw_orders_ab2
select
    md5(cast(coalesce(cast("id" as text), '') || '-' || coalesce(cast(status as text), '') || '-' || coalesce(cast(user_id as text), '') || '-' || coalesce(cast(order_date as text), '') as text)) as _airbyte_raw_orders_hashid,
    tmp.*
from __dbt__cte__raw_orders_ab2 tmp
-- raw_orders
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__raw_orders_ab3
select
    "id" as order_id,
    status,
    user_id as customer_id,
    order_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_raw_orders_hashid
from __dbt__cte__raw_orders_ab3
-- raw_orders from "ETL_source".public._airbyte_raw_raw_orders
where 1 = 1
  );