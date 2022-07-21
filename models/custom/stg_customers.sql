

  create  table "ETL_source".public."raw_customers__dbt_tmp"
  as (
    
with __dbt__cte__raw_customers_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "ETL_source".public._airbyte_raw_raw_customers
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'last_name') as last_name,
    jsonb_extract_path_text(_airbyte_data, 'first_name') as first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "ETL_source".public._airbyte_raw_raw_customers as table_alias
-- raw_customers
where 1 = 1
),  __dbt__cte__raw_customers_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__raw_customers_ab1
select
    cast("id" as 
    float
) as "id",
    cast(last_name as text) as last_name,
    cast(first_name as text) as first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__raw_customers_ab1
-- raw_customers
where 1 = 1
),  __dbt__cte__raw_customers_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__raw_customers_ab2
select
    md5(cast(coalesce(cast("id" as text), '') || '-' || coalesce(cast(last_name as text), '') || '-' || coalesce(cast(first_name as text), '') as text)) as _airbyte_raw_customers_hashid,
    tmp.*
from __dbt__cte__raw_customers_ab2 tmp
-- raw_customers
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__raw_customers_ab3
select
    "id" as customer_id,
    last_name,
    first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_raw_customers_hashid
from __dbt__cte__raw_customers_ab3
-- raw_customers from "ETL_source".public._airbyte_raw_raw_customers
where 1 = 1
  );