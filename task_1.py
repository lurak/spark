def task_1(spark, input_file):
    df = spark.read.csv(input_file, header=True)
    ids = df.filter("char_length(video_id) == 11 and video_id != 'ABOUT WIRED'")\
        .select("video_id", "title", "description")
    top_ten = ids.groupBy('video_id', 'title', 'description')\
        .count()\
        .sort('count', ascending=False)\
        .limit(10)
    data = top_ten.rdd.map(lambda x: [x['video_id'], x['title'], x['description']])
    ids = data.map(lambda x: x[0])
    data = data.collect()
    for i in range(len(ids.collect())):
        row = df.where(df.video_id == ids.collect()[i])
        rev = row.sort('trending_date', ascending=False)
        latest = rev.first()
        n_df = row.select('trending_date', 'views', 'likes', 'dislikes')
        n_df = n_df.rdd.map(lambda x: [x['trending_date'], x['views'], x['likes'], x['dislikes']])
        n_df = n_df.collect()
        res = list(map(lambda x: {
            "date": x[0],
            "views": x[1],
            "likes": x[2],
            "dislikes": x[3]
        }, n_df))
        data[i].extend([latest.views, latest.likes, latest.dislikes, res])
    res = list(map(lambda x: {
        "id": x[0],
        "title": x[1],
        "description": x[2],
        "latest_views": x[3],
        "latest_likes": x[4],
        "latest_dislikes": x[5],
        "trending_days": x[6]
    }, data))
    result = {"videos": res}
    return result
