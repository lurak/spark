def sort_fun(x):
    try:
        x = -x['count']
    except TypeError:
        x = 0
    return x


def reduce_fun(x, y):
    x["count"] += y["count"]
    x["video_ids"] += y["video_ids"]
    return x


def reduce_fun_1(x, y):
    if len(x["tags"]) < 10:
        x["tags"] += y["tags"]
    return x


def map_fun(x):
    x[1]['start_date'] = x[0]
    return x[1]


def task_3(spark, input_file):
    df = spark.read.csv(input_file, header=True)
    df = df.filter("char_length(video_id) == 11 and video_id != 'ABOUT WIRED' and tags is not null"
                   " and views is not null and trending_date is not null")\
        .select('trending_date', 'video_id', 'tags')
    df = df.rdd
    df = df.map(lambda x: ((x[0][:2] + x[0][-3:], x[1]), x[2].split('|'))) \
        .reduceByKey(lambda x, y: x) \
        .flatMapValues(lambda x: x)
    df = df.map(lambda x: ((x[0][0], x[1]), {
                 "video_ids": [x[0][1]],
                 "count": 1})) \
        .reduceByKey(reduce_fun) \
        .sortBy(sort_fun) \
        .map(lambda x: (x[0][0], {"tags": [{
             "tag": x[0][1],
             "count": x[1]["count"],
             "video_ids": x[1]["video_ids"]
                                            }]}))\
        .reduceByKey(reduce_fun_1)
    df = df.map(map_fun).take(10)
    res = {"months": df}
    return res
