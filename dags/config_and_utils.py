from params import KEYFILE_PATH_DBT_BQ, BASE_URL, GCP_PROJECT

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

    month = int(exec_date[5:7])
    year = int(exec_date[0:4])
    response = requests.get(BASE_URL + f'{year}-{month:02d}.parquet')

    while response.status_code != 200:
        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1
            response = requests.get(BASE_URL + f'{year}-{month:02d}.parquet')

    print(f'✅ Data found in {month:02d}/{year}')

    with open(f'/usr/local/airflow/tmp/data_{exec_date}.parquet', 'wb') as f:
        f.write(response.content)
