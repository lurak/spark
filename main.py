import sys
import json
from pyspark.sql import SparkSession
from task_1 import task_1
from task_3 import task_3
from task_4 import task_4
from task_5 import task_5
from task_6 import task_6


if __name__ == "__main__":
    input_file, output_file = sys.argv[1], sys.argv[2]
    spark = SparkSession \
        .builder \
        .appName("Spark") \
        .getOrCreate()
    res_1 = task_1(spark, input_file)
    with open(output_file, 'w') as outfile:
        json.dump(res_1, outfile, indent=4)
    res_4 = task_4(spark, input_file)
    with open(output_file, 'a') as outfile:
        json.dump(res_4, outfile, indent=4)
    res_5 = task_5(spark, input_file)
    with open(output_file, 'a') as outfile:
        json.dump(res_5, outfile, indent=4)
    res_6 = task_6(spark, input_file)
    with open(output_file, 'a') as outfile:
        json.dump(res_6, outfile, indent=4)
    res_3 = task_3(spark, input_file)
    with open(output_file, 'a') as outfile:
        json.dump(res_3, outfile, indent=4)
