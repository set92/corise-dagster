from typing import List

from dagster import Nothing, asset, with_resources, In, Out, OpExecutionContext, define_asset_job, AssetSelection
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    op_tags={"kind": "s3"},
    group_name = "corise",
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    output = list()
    s3_key = context.op_config['s3_key']
    s3_data = context.resources.s3.get_data(s3_key)
    for row in s3_data:
        context.log.debug(f"processing {row=}")
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    op_tags={"kind": "processing"},
    group_name = "corise",
    compute_kind = "PyCalc",
    description="Given a list of stocks return the Aggregation with the greatest high value"
)
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    max_stock: Stock = max(get_s3_data, key=lambda x: x.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@asset(
    required_resource_keys={'redis'},
    op_tags={"kind": "redis"},
    group_name = "corise",
    description="Post aggregate result to Redis",
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation) -> None:
    context.log.debug(f"Putting {process_data} to Redis")
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource,
                   "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        }
    }
)
