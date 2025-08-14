from pyspark.sql import SparkSession
from model_test.adapters import SparkAdapter
from model_test.core import LocalFile, SQLFile
from model_test.core import TestModelSQL, TestExecution

adapter = SparkAdapter(spark=SparkSession.builder.getOrCreate())
test_csv = LocalFile(path="/Users/vitorhenriquemendes/Documents/projects/pytest_class/test.csv")
expected_csv = LocalFile(path="/Users/vitorhenriquemendes/Documents/projects/pytest_class/test2.csv")
sql_file = SQLFile(path="/Users/vitorhenriquemendes/Documents/projects/pytest_class/teste.sql")

test = TestModelSQL(
    inputs={
        "dataset1": test_csv,
    },
    expected=expected_csv,
    backend=adapter,
    load_params={"inferSchema": True, "header":True},
    run_params={
        "my_model": "dataset1"
    },
    sql=sql_file
)

TestExecution.test_model(test)
