

  create  table "ETL_source".public."raw_payments__dbt_tmp"
  as (
    
with __dbt__cte__raw_payments_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "ETL_source".public._airbyte_raw_raw_payments
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'amount') as amount,
    jsonb_extract_path_text(_airbyte_data, 'order_id') as order_id,
    jsonb_extract_path_text(_airbyte_data, 'payment_method') as payment_method,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "ETL_source".public._airbyte_raw_raw_payments as table_alias
-- raw_payments
where 1 = 1
),  __dbt__cte__raw_payments_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__raw_payments_ab1
select
    cast("id" as 
    float
) as "id",
    cast(amount as 
    float
) as amount,
    cast(order_id as 
    float
) as order_id,
    cast(payment_method as text) as payment_method,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__raw_payments_ab1
-- raw_payments
where 1 = 1
),  __dbt__cte__raw_payments_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__raw_payments_ab2
select
    md5(cast(coalesce(cast("id" as text), '') || '-' || coalesce(cast(amount as text), '') || '-' || coalesce(cast(order_id as text), '') || '-' || coalesce(cast(payment_method as text), '') as text)) as _airbyte_raw_payments_hashid,
    tmp.*
from __dbt__cte__raw_payments_ab2 tmp
-- raw_payments
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__raw_payments_ab3
select
    "id" as payment_id,
    amount,
    order_id,
    payment_method,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_raw_payments_hashid
from __dbt__cte__raw_payments_ab3
-- raw_payments from "ETL_source".public._airbyte_raw_raw_payments
where 1 = 1
  );