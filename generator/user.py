#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# user
import numpy as np
from numpy.random import choice
from games import pick_random_game, get_score_and_price

#################### Constants Start ####################
GENRES = [
    'Indie',
    'Action',
    'Adventure',
    'Casual',
    'Strategy',
    'RPG',
    'Simulation',
    'Sports',
    'Racing',
    'MassivelyMultiplayer',
]

GENRE_PROB = [
    0.16316077314816888,
    0.32103943217694136,
    0.12057636491346992,
    0.05386579336777881,
    0.10387719738974585,
    0.1016452846990327,
    0.05952845400888355,
    0.01331618874980745,
    0.01134042010429616,
    0.051650091441875344,
]

GENRE_NUM_PROB = [0.1, 0.2, 0.3, 0.2, 0.1, 0.09, 0.005, 0.004, 0.001]
USER_META_TYPES = ['adventurousness', 'wealthiness', 'activeness']
USER_META_RANGE = [0.01, 0.02, 0.03, 0.05, 0.1, 0.2, 0.3, 0.5]
USER_META_PROB = [0.05, 0.1, 0.1, 0.17, 0.25, 0.18, 0.1, 0.05]

INIT_RATING_MULTI = 1.0 / 50.0
WEALTHINESS_MULTI = 1000
PLAY_TIME_MU = 0.5
PLAY_TIME_SIGMA = 1.0
TIME_TO_RATE = 1.0
MAX_TRIAL = 100
#################### Constants End####################

class User(object):
    '''The User class'''
    def __init__(self, user_id, user_profile=None):
        self.data = {}
        self.id = user_id
        self.profile = user_profile
        self.create_random_user()

    def create_random_user(self):
        # randomly assign user metadata
        for meta in USER_META_TYPES:
            self.data[meta] = choice(USER_META_RANGE, p=USER_META_PROB)

        self.data['adventurousness'] = self.data['adventurousness'] ** 0.5 / 2

        # randomly 'like' a few genres
        self.data['genre_affinity'] = [0.0] * len(GENRES) # init
        genre_num = choice(range(1, len(GENRE_NUM_PROB) + 1), p=GENRE_NUM_PROB)
        affinity = list(np.random.dirichlet(np.ones(genre_num), size=1)[0])
        while affinity:
            genre = choice(range(len(GENRES)), p=GENRE_PROB)
            if self.data['genre_affinity'][genre] > 0:
                continue

            self.data['genre_affinity'][genre] = affinity.pop()

        # other fields
        self.data['library'] = {'id': [],
                                'rating': [],
                                'rating_sum': 0.0,
                                'play_time': [],
                                'play_time_sum': 0.0}

    def take_random_action(self):
        # take random action based on user attributes
        if np.random.random_sample() < self.data['adventurousness'] or \
           self.data['library']['rating_sum'] == 0:
            act, (gid, val) = 'buy', self.buy_random_game()
        else:
            act, (gid, val) = 'play', self.play_random_game()

        if val > -1:
            return act, (gid, val)

    def pick_random_genre(self):
        if np.random.random_sample() < self.data['adventurousness']:
            return choice(GENRES)
        return choice(GENRES, p=self.data['genre_affinity'])

    def buy_game(self, game_id, init_rating):
        self.data['library']['id'].append(game_id)
        self.data['library']['rating'].append(init_rating)
        self.data['library']['rating_sum'] += init_rating
        self.data['library']['play_time'].append(0.0)
        return game_id

    def buy_random_game(self):

        # max_price willing to pay
        max_price = self.data['wealthiness'] * \
                    np.random.randint(WEALTHINESS_MULTI)

        for _ in range(MAX_TRIAL):

            # randomly pick a game
            game_id = pick_random_game(self.pick_random_genre())
            init_rating, price = get_score_and_price(game_id)
            init_rating *= INIT_RATING_MULTI

            # skip if too expensive or already brought
            if price > max_price or game_id in self.data['library']['id']:
                continue

            return self.buy_game(game_id, init_rating), price

        return -1, -1 # reach MAX_TRIAL

    def play_random_game(self):
        if self.data['library']['rating_sum'] == 0.0:
            return -1

        # probility vector for owned games
        prob = [r / self.data['library']['rating_sum'] \
                for r in self.data['library']['rating']]

        idx = choice(range(len(self.data['library']['id'])), p=prob)
        game_id = self.data['library']['id'][idx]
        rating = self.data['library']['rating'][idx]

        # generate normally distributed playtime
        offset = np.log10(rating * ((self.data['activeness'] * 100) ** 0.2))
        mu = PLAY_TIME_MU * offset
        sigma = PLAY_TIME_SIGMA + offset
        play_time = round(np.random.normal(mu, sigma) * 60, 2)
        if play_time <= 0.5:
            return game_id, -1

        pre_time = self.data['library']['play_time'][idx]
        self.data['library']['play_time'][idx] += play_time
        self.data['library']['play_time_sum'] += play_time
        if pre_time < 1:
            add_rating = np.log10(play_time) * TIME_TO_RATE
        else:
            add_rating = np.log10(1.0 + play_time / pre_time) * TIME_TO_RATE

        self.data['library']['rating'][idx] += add_rating
        self.data['library']['rating_sum'] += add_rating

        return game_id, play_time
