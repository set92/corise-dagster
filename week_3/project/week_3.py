from typing import List

from dagster import (In, Nothing, Out, ResourceDefinition, RetryPolicy, RunRequest, ScheduleDefinition, SkipReason,
                     graph, op, sensor, static_partitioned_config, get_dagster_logger, )
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


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
def week_3_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


partition_keys = [str(i) for i in range(1, 11)]


@static_partitioned_config(partition_keys=partition_keys)
def docker_config(partition_key: str):
    key = f'prefix/stock_{partition_key}.csv'
    return {
        "resources": {**docker["resources"]},
        "ops": {"get_s3_data": {"config": {"s3_key": key}}}
    }


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor(
    job=docker_week_3_pipeline,
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    bucket, _, _, endpoint_url = docker["resources"]["s3"]["config"].values()
    new_files = get_s3_keys(bucket=bucket,
                            prefix='prefix',
                            endpoint_url=endpoint_url)
    log = get_dagster_logger()
    log.info(f'RunRequest for {new_files}')
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={
                "resources": {**docker["resources"]},
                "ops": {"get_s3_data": {"config": {"s3_key": new_file}}}
            }
        )

