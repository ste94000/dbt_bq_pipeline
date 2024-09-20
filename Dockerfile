FROM quay.io/astronomer/astro-runtime:12.1.0-python-3.10
COPY /keyfile /keyfile
RUN mkdir tmp && python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-bigquery && deactivate
