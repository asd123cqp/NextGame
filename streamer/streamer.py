#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# consumer.py
import json
from collections import Counter, deque
from os import getenv
from redis import StrictRedis
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

BROKERS = {"metadata.broker.list": getenv('BROKERS', '10.0.0.12:9092')}
REDIS_HOST = getenv('REDIS_HOST', '10.0.0.11')
REDIS_PORT = int(getenv('REDIS_PORT', 6379))
MAX_USER_BATCH = 100
MAX_ACTION_BATCH = 1000

def pipe_exec(partition, db_idx, func, batch_size=1):
    r = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=db_idx)
    pipe, batch = r.pipeline(), deque()
    for record in partition:
        if len(batch) >= batch_size:
            func(batch, pipe)
            batch = deque()

        batch.append(record)

    func(batch, pipe)
    pipe.execute()

# new users
def add_user(batch, pipe):
    for u in batch:
        user_profile = json.loads(u[1])
        pipe.hmset(u[0], user_profile)

# buy
def buy_game(batch, pipe):
    for user, records in batch:
        for game_id in records:
            pipe.hincrbyfloat(user, game_id, 0.0)
        pipe.hset(user, 'UPDATED', True)

# combine 2 sets of play records for one player
def merge_play_record(r1, r2):
    r1.update(r2)
    return r1

# play
def play_game(partition, pipe):
    for user, records in partition:
        for game_id, val in records.items():
            pipe.hincrbyfloat(user, game_id, val)
        pipe.hset(user, 'UPDATED', True)

def main():
    # init
    sc = SparkContext(appName="Test")
    ssc = StreamingContext(sc, 2)
    sc.setLogLevel('ERROR')

    # new players joined the community
    join = KafkaUtils.createDirectStream(ssc, ['join'], BROKERS)
    join.foreachRDD(lambda rdd: rdd.foreachPartition( \
                    lambda p: pipe_exec(p, 1, add_user)))

    # players purchased some games
    buy = KafkaUtils.createDirectStream(ssc, ['buy'], BROKERS)
    buy.transform(lambda rdd: rdd.map(lambda x: (x[0], x[1].split(',')))) \
       .filter(lambda line: len(line[1]) == 2) \
       .map(lambda rec: (rec[0], rec[1][0])) \
       .groupByKey() \
       .mapValues(list) \
       .foreachRDD(lambda rdd: rdd.foreachPartition( \
                   lambda p: pipe_exec(p, 2, buy_game, MAX_USER_BATCH)))

    # players played some games
    play = KafkaUtils.createDirectStream(ssc, ['play'], BROKERS)
    play.transform(lambda rdd: rdd.map(lambda x: (x[0], x[1].split(',')))) \
        .filter(lambda line: len(line[1]) == 2) \
        .map(lambda rec: (rec[0], Counter({rec[1][0]: float(rec[1][1])}))) \
        .reduceByKey(merge_play_record) \
        .foreachRDD(lambda rdd: rdd.foreachPartition( \
                    lambda p: pipe_exec(p, 2, play_game, MAX_ACTION_BATCH)))

    # start streaming
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
