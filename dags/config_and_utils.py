from params import KEYFILE_PATH_DBT_BQ, BASE_URL, GCP_PROJECT
from datetime import date
from dateutil.relativedelta import relativedelta
import requests

from cosmos import ProjectConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Dbt bq service account config
profile_config = ProfileConfig(
    profile_name='default',
    target_name='dev',
    profile_mapping= GoogleCloudServiceAccountFileProfileMapping(
        conn_id= 'bqconn',
        profile_args={'project': GCP_PROJECT, 'dataset': 'taxidb', 'keyfile': KEYFILE_PATH_DBT_BQ}
    )
)

project_config = ProjectConfig('/usr/local/airflow/dags/dbt/dbt_projet')

# Download file
def download_file(exec_date):

    d = date(int(exec_date[:4]), int(exec_date[5:7]), 1)

    while True:
        response = requests.get(f'{BASE_URL}{d:%Y-%m}.parquet')
        if response.status_code == 200:
            break
        d -= relativedelta(months=1)

    print(f'✅ Data found in {d:%m/%Y}')

    with open(f'/usr/local/airflow/tmp/data_{exec_date}.parquet', 'wb') as f:
        f.write(response.content)
