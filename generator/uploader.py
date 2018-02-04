#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# uploader

import pickle, redis, os
import numpy as np
RATING_MULTI = 1.0 / 50.0
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_HOST = os.getenv('REDIS_HOST', '10.0.0.5')
INT_FIELDS = ('PlayersEstimate', 'Owners', 'RecommendationCount')
FLOAT_FIELDS = ('PriceInitial', 'PriceFinal', 'InitialRating')
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

with open('data/game_data.pkl', 'rb') as f:
    db = pickle.load(f)['game_info']

print(len(db))
# for gid, ginfo in enumerate(db):
#     ginfo['InitialRating'] = ginfo['Metacritic'] * RATING_MULTI
#     for field in INT_FIELDS:
#         ginfo[field] = int(ginfo.get(field, 0))
#     for field in FLOAT_FIELDS:
#         ginfo[field] = float(ginfo.get(field, 0))
#     r.hmset(gid, ginfo)

# rec_count = [r['RecommendationCount'] for r in db]
# print([r for _, r in zip(range(12), np.argsort(rec_count)[::-1])])
