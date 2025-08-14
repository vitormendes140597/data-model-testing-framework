import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture
def local_spark_session():
    spark = (
        SparkSession.getActiveSession() or
        SparkSession.builder.master("local[*]").appName("Model Testing").getOrCreate()
    )

    yield spark

    spark.stop()
