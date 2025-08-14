from model_test.adapters.adapter import BackendAdapter
from model_test.core.utils import persist_as_temp_view

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

import pandas as pd

class SparkAdapter(BackendAdapter):

    def __init__(self, spark: SparkSession):
        super().__init__()
        self.spark = spark

    @classmethod
    def to_pandas(cls, df: DataFrame) -> pd.DataFrame:
        print(type(df))
        return df.toPandas()

    @persist_as_temp_view
    def read_csv(
        self,
        path: str,
        sep: str = ',',
        encoding: str = 'UTF-8',
        quote: str = '"',
        header: bool = False,
        infer_schema: bool = False,
        multi_line: bool = False,
        schema: StructType = None,
        temp_view: str = None,
        **kwargs
    ) -> DataFrame:
        reader = self.spark.read.format('csv')
        params = list(locals().items())[1:]

        for param, value in params:
            reader = reader.option(param, value)

        return reader.load()

    @persist_as_temp_view
    def read_json(self, **kwargs):
        return super().read_json(**kwargs)

    def execute_sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)
