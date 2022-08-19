import csv
import heapq
from datetime import datetime
from typing import List

from dagster import (DynamicOut, DynamicOutput, In, Out, job, op, usable_as_dagster_type, )
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )

    def __lt__(self, other):
        return self.high < other.high

@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output



@op(
    config_schema={"nlargest": int},
    ins={"stocks": In(dagster_type=List[Stock])},
    out=DynamicOut(dagster_type = Aggregation),
    description="Given a list of stocks return the Aggregation with the greatest high value"
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    n_aggregations = context.op_config["nlargest"]
    lst_stock = heapq.nlargest(n_aggregations, stocks)
    for idx in range(n_aggregations):
        max_stock = lst_stock[idx]
        yield DynamicOutput(Aggregation(date=max_stock.date, high=max_stock.high), mapping_key = str(idx))


@op(description = "Upload an Aggregation to Redis",
    tags={"kind": "redis"})
def put_redis_data(context, aggregation: Aggregation) -> None:
    context.log.info(f"{aggregation=}")
    pass


@job
def week_1_pipeline():
    dynamic_output = process_data(get_s3_data())
    dynamic_data = dynamic_output.map(put_redis_data)
    dynamic_data.collect()
    pass
