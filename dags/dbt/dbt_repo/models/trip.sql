{{ config(materialized='incremental', unique_key='trip_id') }}

select distinct
{{ dbt_utils.generate_surrogate_key([ 'st.tpep_pickup_datetime',  'st.tpep_dropoff_datetime',  'st.PULocationID',  'st.DOLocationID',  'st.total_amount',  'st.payment_type',  'st.VendorID',  'st.RatecodeID',  'saf.store_and_fwd_flag',  'st.trip_distance',  'st.fare_amount']) }} as trip_id,
st.tpep_pickup_datetime as pickup_datetime,
st.tpep_dropoff_datetime as dropoff_datetime,
st.VendorID,
st.RatecodeID,
st.PULocationID,
st.DOLocationID,
saf.store_and_fwd_id,
st.payment_type,
st.trip_distance,
st.fare_amount,
st.extra,
st.mta_tax,
st.tip_amount,
st.tolls_amount,
st.improvement_surcharge,
st.total_amount,
st.congestion_surcharge,
st.Airport_fee
from {{ source('raw', 'taxi_start') }} as st
join {{ source('taxidb', 'store_and_forward') }} as saf on saf.store_and_fwd_flag = st.store_and_fwd_flag
where extract(year from st.tpep_pickup_datetime) > 2023
and st.passenger_count > 0
and st.fare_amount > 2.5
and st.trip_distance > 0
and st.payment_type < 4
and st.tpep_pickup_datetime IS NOT NULL
and st.tpep_dropoff_datetime IS NOT NULL
and st.PULocationID IS NOT NULL
and st.DOLocationID IS NOT NULL
and st.passenger_count IS NOT NULL
and st.trip_distance IS NOT NULL
and st.payment_type IS NOT NULL
and st.fare_amount IS NOT NULL
and st.total_amount IS NOT NULL
{% if is_incremental() %}
and st.tpep_pickup_datetime > (select max(tpep_pickup_datetime) from {{ this }})
{% endif %}
