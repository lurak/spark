from pyspark.sql import functions as F


def sort_fun(x):
    try:
        x = -x
    except TypeError:
        x = 0
    return x


def reduce_fun(x, y):
    if len(x["videos"]) < 10:
        x["videos"] += y["videos"]
    return x


def task_6(spark, input_file):
    df = spark.read.csv(input_file, header=True)
    df = df.filter("char_length(video_id) == 11 and video_id != 'ABOUT WIRED'")
    df = df.filter("views > 100000")\
        .select("video_id", 'title', 'category_id', df['likes']/df['dislikes'])\
        .withColumnRenamed('(likes / dislikes)', 'ratio')
    df = df.groupBy('video_id', 'title', 'category_id')\
        .agg(F.max(df.ratio).alias('Mratio'))
    df = df.rdd
    df = df.sortBy(sort_fun).map(lambda x: (x[2], {"category_id": x[2],
                                            "videos": [{"video_id": x[0],
                                                        "video_title": x[1],
                                                        "ratio": x[3]}]}))\
        .reduceByKey(reduce_fun).take(10)
    res = list(map(lambda x: x[1], df))
    res = {"categories": res}
    return res
