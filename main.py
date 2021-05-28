import sys
import json
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from task_1 import task_1
from task_3 import task_3
from task_4 import task_4
from task_5 import task_5
from task_6 import task_6


if __name__ == "__main__":
    input_file, output_file = sys.argv[1], sys.argv[2]
    sc = SparkContext('local', 'test')
    spark = SparkSession \
        .builder \
        .appName("Spark") \
        .getOrCreate()
    res_1 = task_1(spark, input_file)
    res_4 = task_4(spark, input_file)
    res_3 = task_3(spark, input_file)
    res_5 = task_5(spark, input_file)
    res_6 = task_6(spark, input_file)
    res_1 = json.dumps(res_1, indent=4)
    res_4 = json.dumps(res_4, indent=4)
    res_3 = json.dumps(res_3, indent=4)
    res_5 = json.dumps(res_5, indent=4)
    res_6 = json.dumps(res_6, indent=4)
    df = spark.read.json(sc.parallelize([res_1, res_3, res_4, res_5, res_6]))
    df.coalesce(1).write.format('json').mode("overwrite").save(output_file)
