import os
import subprocess

print("1. Running Bronze Layer ")
subprocess.run(["/opt/spark/bin/spark-submit", "transformations/1st_bronze.py"])

print("2. Running Silver Layer ")
subprocess.run(["/opt/spark/bin/spark-submit", "transformations/2nd_silver.py"])

print("3. Loading Warehouse ")
subprocess.run(["/opt/spark/bin/spark-submit", "transformations/4th_load_warehouse.py"])

print("4. Running Gold Layer ")
subprocess.run(["/opt/spark/bin/spark-submit", "transformations/3rd_gold.py"])

print("!! Batch Pipeline Completed Successfully !!")
