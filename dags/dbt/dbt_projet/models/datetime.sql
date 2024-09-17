{{ config(materialized='incremental') }}

select
tpep_pickup_datetime as pickup_datetime,
tpep_dropoff_datetime as dropoff_datetime,
extract(HOUR from tpep_pickup_datetime) AS pickup_hour,
FORMAT_TIMESTAMP('%A', tpep_pickup_datetime) AS pickup_weekday,
extract(DAY from tpep_pickup_datetime) AS pickup_day,
extract(MONTH from tpep_pickup_datetime) AS pickup_month,
extract(YEAR from tpep_pickup_datetime) AS pickup_year,
extract(HOUR from tpep_dropoff_datetime) AS dropoff_hour,
FORMAT_TIMESTAMP('%A', tpep_dropoff_datetime) AS dropoff_weekday,
extract(DAY from tpep_dropoff_datetime) AS dropoff_day,
extract(MONTH from tpep_dropoff_datetime) AS dropoff_month,
extract(YEAR from tpep_dropoff_datetime) AS dropoff_year

from {{ ref('start_view') }}

{% if is_incremental() %}
where tpep_pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
