from pyspark.sql import SparkSession
from transformations.bronze_layer import run_bronze
from transformations.silver_layer import run_silver
from transformations.gold_layer import run_gold
from transformations.warehouse_layer import run_warehouse


def run_pipeline():

    spark = (
        SparkSession.builder
        .appName("FullBatchPipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Running Bronze")
        run_bronze(spark)

        print("Running Silver")
        run_silver(spark)

        print("Running Warehouse")
        run_warehouse(spark)

        print("Running Gold")
        run_gold(spark)

        print("Pipeline completed successfully")

    except Exception as e:
        print("Pipeline failed")
        print(str(e))
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
