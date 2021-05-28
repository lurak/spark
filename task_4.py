from pyspark.sql import functions as F


def int_fun(x):
    try:
        x = int(x)
    except (TypeError, ValueError):
        x = 0
    return x


def reduce_fun(x, y):
    x['total_views'] += y['total_views']
    x["video_ids"] += y["video_ids"]
    return x


def task_4(spark, input_file):
    df = spark.read.csv(input_file, header=True)
    df = df.filter("char_length(video_id) == 11 and video_id != 'ABOUT WIRED'")
    ch = df.select('channel_title', 'views', 'trending_date', 'video_id') \
        .groupBy('channel_title', 'video_id') \
        .agg(F.max(df.trending_date).alias('MTD'))
    views = df.select('views', 'trending_date', 'video_id') \
        .withColumnRenamed('trending_date', 'td') \
        .withColumnRenamed('video_id', 'id')
    ch_join = ch.join(views, [ch.MTD == views.td, ch.video_id == views.id]) \
        .select('channel_title', 'video_id', 'MTD', 'views')
    ch_join = ch_join.rdd
    ch_join = ch_join.map(lambda x: (x['channel_title'], {"total_views": int_fun(x[3]),
                                                          "end_date": x[2],
                                                          "video_ids": [{
                                                           "video_id": x[1],
                                                           "video_view": x[3]
                                                          }]})) \
        .reduceByKey(reduce_fun) \
        .sortBy(lambda x: -x[1]["total_views"]) \
        .take(20)
    res = list(map(lambda x: {"channel_title": x[0],
                              "total_views": x[1]["total_views"],
                              "end_date": x[1]["end_date"],
                              "video_ids": x[1]["video_ids"]}, ch_join))
    res = {"channels": res}
    return res
