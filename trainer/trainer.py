#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# trainer
import pickle, numpy, db
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel

BATCH_SIZE = 100
RANK = 10
ITERATION = 5
MODEL_DEST = 'model.pkl'

def main():

    # init
    sc = SparkContext(appName="Test")
    sc.setLogLevel('ERROR')

    # retrieve user data from redis, chunk by chunk
    ratings = sc.parallelize([])
    for batch in db.fetch_all_user_lib(BATCH_SIZE):
        ratings = ratings.union(sc.parallelize(batch))

    print('Training...')

    ###########################
    # training_RDD, test_RDD = ratings.randomSplit([9, 1])
    # model = ALS.trainImplicit(training_RDD, RANK, ITERATION)
    # model = ALS.train(training_RDD, RANK, ITERATION)
    # testdata = test_RDD.map(lambda p: (p[0], p[1]))
    # predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    # ratesAndPreds = test_RDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    # error = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean() ** 0.5
    # print("Error = %f at rank %d." %(error, RANK))
    #########################

    # extract model
    recommender = ALS.train(ratings, RANK, ITERATION)
    # recommender = MatrixFactorizationModel.load(sc, "als.model")
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
