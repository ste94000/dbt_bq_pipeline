{{ config(materialized='incremental') }}

select
{{ dbt_utils.generate_surrogate_key(['st.tpep_pickup_datetime', 'st.tpep_dropoff_datetime', 'st.PULocationID', 'st.DOLocationID', 'st.total_amount', 'st.payment_type', 'st.VendorID']) }} as trip_id,
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
from {{ ref('start_view') }} as st
join `taxidb.store_and_forward` as saf on saf.store_and_fwd_flag = st.store_and_fwd_flag
/*join {{ ref('datetime') }} as da on st.tpep_pickup_datetime = da.pickup_datetime
and st.tpep_dropoff_datetime = da.dropoff_datetime*/

{% if is_incremental() %}
where st.tpep_pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
