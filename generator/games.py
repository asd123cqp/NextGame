#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# game
import numpy as np
import pickle
from numpy.random import choice

with open('data/game_data.pkl', 'rb') as f:
    db = pickle.load(f)

def pick_random_game(genre):
    return choice(db['genre_game_idx'][genre], p=db['genre_game_prob'][genre])

def get_score_and_price(game_id):
    return db['game_info'][game_id]['Metacritic'], \
           db['game_info'][game_id]['PriceFinal']
