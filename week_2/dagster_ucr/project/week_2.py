import csv
from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    ins=None,
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context) -> [Stock]:
    output = list()
    s3_key = context.op_config['s3_key']
    s3_data = context.resources.s3.get_data(s3_key)
    for row in s3_data:
        context.log.debug(f"processing {row=}")
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    tags={"kind": "processing"},
    description="Given a list of stocks return the Aggregation with the greatest high value"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    max_stock: Stock = max(stocks, key=lambda x: x.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    required_resource_keys={'redis'},
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=None,
    tags={"kind": "redis"},
    description="Post aggregate result to Redis",
)
def put_redis_data(context, aggregation: Aggregation) -> None:
    context.log.debug(f"Putting {aggregation} to Redis")
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


@graph
def week_2_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
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
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
