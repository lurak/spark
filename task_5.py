def reduce_fun(x, y):
    x['ttd'] += y['ttd']
    x['videos'] += y['videos']
    return x


def task_5(spark, input_file):
    df = spark.read.csv(input_file, header=True)
    df = df.filter("char_length(video_id) == 11 and video_id != 'ABOUT WIRED'")
    ch = df.groupBy('channel_title', 'video_id', 'title')\
        .count()
    ch = ch.rdd
    ch = ch.map(lambda x: (x["channel_title"], {"channel_name": x['channel_title'],
                                                "ttd": x["count"],
                                                "videos": [{"video_id": x["video_id"],
                                                            "video_name": x["title"]}]}))\
        .reduceByKey(reduce_fun)\
        .sortBy(lambda x: -x[1]['ttd'])\
        .take(10)
    res = list(map(lambda x: x[1], ch))
    res = {"channels": res}
    return res
