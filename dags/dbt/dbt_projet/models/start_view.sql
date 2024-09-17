{{ config(materialized='incremental') }}

select distinct *
from `taxi_db.start_table`
where extract(year from tpep_pickup_datetime) > 2023
and passenger_count > 0
and fare_amount > 2.5
and trip_distance > 0
and payment_type < 4
and tpep_pickup_datetime IS NOT NULL
and tpep_dropoff_datetime IS NOT NULL
and passenger_count IS NOT NULL
and trip_distance IS NOT NULL
and RateCodeID IS NOT NULL
and payment_type IS NOT NULL
and fare_amount IS NOT NULL
and extra IS NOT NULL
and mta_tax IS NOT NULL
and tip_amount IS NOT NULL
and tolls_amount IS NOT NULL
and improvement_surcharge IS NOT NULL
and total_amount IS NOT NULL
and congestion_surcharge IS NOT NULL
and Airport_fee IS NOT NULL

{% if is_incremental() %}
and tpep_pickup_datetime > (select max(tpep_pickup_datetime) from {{ this }})
{% endif %}
