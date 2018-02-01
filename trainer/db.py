#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# db

import pickle, json
from os import getenv
from redis import StrictRedis
from math import log10
from boto3 import client
from time import time

MODEL_BUCKET = 'nextgame-models'
DB_INDEX = {
    'game': 0,
    'user': 1,
    'library': 2,
    'recommendation': 3,
}

# REDIS_HOST = getenv('REDIS_HOST', '52.70.7.10')
# REDIS_HOST = getenv('REDIS_HOST', '10.0.0.11')
REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(getenv('REDIS_PORT', 6379))
# INT_FIELDS = ('PlayersEstimate', 'Owners', 'RecommendationCount')
# FLOAT_FIELDS = ('Metacritic', 'PriceInitial', 'PriceFinal', 'InitialRating')
FLOAT_FIELDS = ('Metacritic', 'PriceInitial', 'PriceFinal', 'InitialRating')
TIME_RATE = 0.5

game_db = {}

def connect_redis(db_name):
    if db_name not in DB_INDEX:
        raise ValueError('No database named %s.' %(db_name))
    return StrictRedis(host=REDIS_HOST,
                       port=REDIS_PORT,
                       db=DB_INDEX[db_name],
                       decode_responses=True)
#########################

def fetch_game_info(gid):
    if gid not in game_db:
        r = connect_redis('game')
        game_db[gid] = r.hgetall(gid)
        # for field in INT_FIELDS:
        #     game_db[gid][field] = int(game_db[gid][field])
        for field in FLOAT_FIELDS:
            game_db[gid][field] = float(game_db[gid][field])

    return game_db[gid]

#########################

def fetch_all_user_ids():
    r = connect_redis('library')
    return r.keys()

def user_updated(key):
    return connect_redis('library').hget(key, 'UPDATED') == 'True'

def fetch_user_lib(keys):
    # createa a pipeline to retrieve data
    r = connect_redis('library')
    p = r.pipeline()
    for key in keys:
        p.hgetall(key)

    # store data in usable format
    ret = []
    for key, user in zip(keys, p.execute()):
        for record in user:
            if record != 'UPDATED':
                gid = int(record)
                rating = fetch_game_info(gid)['InitialRating']
                if float(user[record]) > 0:
                    rating += log10(float(user[record])) * TIME_RATE
                ret.append((int(key), gid, rating))

    return ret

def fetch_lib_simple(key):
    # createa a pipeline to retrieve data
    user = connect_redis('library').hgetall(key)
    return [(int(key), int(r), float(user[r])) for r in user if r != 'UPDATED']

def fetch_all_user_lib(batch_size=1):
    all_keys = fetch_all_user_ids()
    for i in range(0, len(all_keys), batch_size):
        yield fetch_user_lib(all_keys[i:i+batch_size])

def fetch_recommendation(key):
    r = connect_redis('recommendation')
    return json.loads(r.hget(key, 'recommendation'))

def update_recommendation(key, u, rec):
    r = connect_redis('recommendation')
    u, rec = json.dumps(u), json.dumps(rec)
    r.hmset(key, {'factor': u, 'recommendation': rec})
    connect_redis('library').hset(key, 'UPDATED', False)

#########################

def upload_model(model):
    try:
        print('Uploading model to S3...')
        s3 = client('s3')
        res = s3.put_object(Bucket=MODEL_BUCKET,
                            Key='model-%d.pkl' %(int(time())),
                            Body=pickle.dumps(model))
        print(res)
    except:
        print('Error uploading model to S3!')

def download_model(stamp):
    try:
        print('Downloading model from S3...')
        s3 = client('s3')
        res = s3.get_object(Bucket=MODEL_BUCKET,
                            Key='model-%d.pkl' %(int(stamp)))['Body'].read()
    except:
        res = None
        print('Error downloading model to S3!')

    return pickle.loads(res)
