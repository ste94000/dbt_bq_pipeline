FROM quay.io/astronomer/astro-runtime:12.0.0
COPY /keyfile /keyfile
RUN mkdir tmp
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-bigquery && deactivate
