#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# recommender
import pickle, json, db
import numpy as np
from multiprocessing.pool import ThreadPool

REC_NUM = 12
DEFAULT_MODEL = 'model-default.pkl'
DEFAULT_REC = [25, 22, 19, 141, 3741, 1692, 1177, 1738, 2343, 21, 1289, 2834]

def get_library(uid):
    lib = db.fetch_lib_simple(uid)
    info = [db.fetch_game_info(game[1]) for game in lib]
    for record, game in zip(lib, info):
        game['PlayTime'] = int(record[2])
    return info

class Recommender(object):
    """docstring for recommender"""
    def __init__(self, default_model=DEFAULT_MODEL):
        with open(default_model, 'rb') as fp:
            self.model = pickle.load(fp)

        self.new_model = None
        self.loading = False

    def load_s3_model(self, timestamp):
        if self.loading:
            return self.new_model

        pool = ThreadPool(processes=1)
        self.loading = True
        self.new_model = pool.apply_async(db.download_model, (timestamp,))
        return self.new_model

    def get_model(self):
        if self.loading:
            try:
                new_model = self.new_model.get(timeout=0.001)
                self.model = new_model
                self.new_model = None
                self.loading = False
                print('New model loaded!')
            except TimeoutError:
                pass
        return self.model

    def recommend(self, uid):
        # check if user was updated
        if not db.user_updated(uid):
            try:
                print('Fetching recommendations from cache...')
                return [db.fetch_game_info(gid) for gid in \
                        db.fetch_recommendation(uid)]
            except TypeError:
                pass

        print('Computing recommendations on the fly...')
        # load model
        m = self.get_model()

        # compose user activity vector, Q
        Q = np.zeros(m['P'].shape[0])
        owned = set()
        for _, gid, rating in db.fetch_user_lib([uid]):
            owned.add(gid)
            Q[m['idx'][gid]] = rating

        # only compute for user who owned at least one game
        if owned:
            # solve user factor matrix
            u = np.linalg.solve(m['PTP'], Q @ m['P'])

            # generate recommendations
            pred, rec = m['P'] @ u, []

            for r in np.argsort(pred)[::-1]:
                gid = m['key'][r]
                if gid not in owned:
                    rec.append(gid)
                if len(rec) == REC_NUM:
                    break
        else:
            rec = DEFAULT_REC
            u = []

        db.update_recommendation(uid, list(u), rec)

        return [db.fetch_game_info(gid) for gid in rec]

if __name__ == '__main__':
    recommender = Recommender()
    print(recommender.recommend(11))
