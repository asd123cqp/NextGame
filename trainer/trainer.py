#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# trainer
import pickle, numpy, db
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel

BATCH_SIZE = 1000
# MAX_RDD_DEPTH = 20
RANK = 10
ITERATION = 5
MODEL_DEST = 'model.pkl'

def main():

    # init
    sc = SparkContext(appName="Test")
    sc.setLogLevel('ERROR')

    # retrieve user data from redis, chunk by chunk
    ratings = []
    # depth = 0
    for batch in db.fetch_all_user_lib(BATCH_SIZE):
        # ratings = ratings.union(sc.parallelize(batch))
        ratings += batch

        # checkpoint long chained rdd to avoid stack overflow
        # if depth >= MAX_RDD_DEPTH:
        #     ratings.checkpoint()
        #     depth = 0
        # depth += 1

    ratings = sc.parallelize(ratings)
    # ratings.checkpoint()
    print('Training...')

    # extract model
    recommender = ALS.train(ratings, RANK, ITERATION)
    pf = recommender.productFeatures().sortByKey().cache()
    pkeys = pf.keys().collect()
    pfeats = numpy.array(pf.values().collect())
    model = {
        'P': pfeats,
        'PTP': pfeats.T @ pfeats,
        'key': pkeys,
        'idx': {val: idx for idx, val in enumerate(pkeys)}
    }

    print('Done! Saving model...')
    db.upload_model(model)
    print('Done! Enjoy!')

if __name__ == '__main__':
    main()
