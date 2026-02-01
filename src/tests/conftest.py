import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Creează o sesiune Spark locală pentru teste."""
    spark = SparkSession.builder.master("local[1]").appName("unit-testing").getOrCreate()
    yield spark
    spark.stop()
