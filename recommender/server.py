#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# server

from recommender import Recommender, get_library
from flask import Flask, jsonify
from kafka import KafkaProducer

KAFKA_SERVER = 'localhost:9092'

app = Flask(__name__)
rec = Recommender()
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, \
                         key_serializer=str.encode, \
                         value_serializer=str.encode)

@app.route('/recommendation/<uid>', methods=['GET'])
def recommendation(uid):
    return jsonify(rec.recommend(int(uid)))

@app.route('/library/<uid>', methods=['GET'])
def library(uid):
    return jsonify(get_library(int(uid)))

@app.route('/buy/<uid>/<gid>', methods=['GET'])
def buy(uid, gid):
    res = producer.send('buy', key=str(uid), value=gid+',0.0')
    return jsonify({'result': 'OK'})

@app.route('/play/<uid>/<gid>', methods=['GET'])
def play(uid, gid):
    res = producer.send('play', key=str(uid), value=gid+',60.0')
    return jsonify({'result': 'Played 60 min'})

if __name__ == '__main__':
    app.run()
