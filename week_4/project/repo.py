from dagster import repository, with_resources, define_asset_job, AssetSelection

# from dagster_dbt import dbt_cli_resource
from project.dbt_config import DBT_PROJECT_PATH
from project.resources import postgres_resource
from project.week_4 import (
    get_s3_data_docker,
    process_data_docker,
    put_redis_data_docker,
)

# from project.week_4_challenge import create_dbt_table, create_dbt_table_docker, dbt_table_docker, final, dbt_assets


temp = define_asset_job(
    "temp",
    selection=AssetSelection.assets(get_s3_data_docker, process_data_docker, put_redis_data_docker),
    config={"ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}},
)


@repository
def repo_content():
    return [get_s3_data_docker, process_data_docker, put_redis_data_docker, temp]


# @repository
# def assets_dbt():
#    return [create_dbt_table_docker, dbt_table_docker, final, *dbt_assets]
