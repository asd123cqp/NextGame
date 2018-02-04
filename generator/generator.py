#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# producer
import json, time
from os import getenv
from multiprocessing import Value, Process
from user import User
from kafka import KafkaProducer
from numpy.random import choice
from faker import Faker

INIT_OFFSET = 1
PROC_NUM = 4
GROUP_PER_PROC = 50000
GROUP_SIZE = 50
ACTION_PER_GROUP = 1000 * GROUP_SIZE
KAFKA_SERVER = getenv('BROKERS', '10.0.0.12:9092')

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def connect_kafka(ks, vs):
    return KafkaProducer(bootstrap_servers=KAFKA_SERVER, \
                         key_serializer=ks, \
                         value_serializer=vs)

def create_user_group(offset):
    with offset.get_lock():
        cur = offset.value
        offset.value += GROUP_SIZE

    # create a group of users
    users, fake = [], Faker()
    producer = connect_kafka(str.encode, json_serializer)
    for i in range(cur, cur + GROUP_SIZE):
        user_profile = fake.simple_profile(sex=None)
        new_user = User(i)
        producer.send('join', key=str(i), value=user_profile)
        users.append(new_user)

    return users

def take_actions(users, action_count):
    new_action_count = 0

    # make random actions
    probs = [user.data['activeness'] for user in users]
    tmp = sum(probs)
    probs = [p / tmp for p in probs]
    producer = connect_kafka(str.encode, str.encode)
    for _ in range(ACTION_PER_GROUP):
        user = choice(users, p=probs)
        res = user.take_random_action()
        if res:
            val_str = ','.join((str(res[1][0]), str(res[1][1])))
            producer.send(res[0], key=str(user.id), value=val_str)
            new_action_count += 1

    with action_count.get_lock():
        action_count.value += new_action_count

    return new_action_count

def worker(offset, action_count, pname):
    for _ in range(GROUP_PER_PROC):
        users = create_user_group(offset)
        new_action_count = take_actions(users, action_count)
        u_first = users[0].id
        print('%s created user #%d~%d, simulated %d actions.' \
               %(pname, u_first, u_first + GROUP_SIZE - 1, new_action_count))

def main():
    processes = []
    offset, action_count = Value('i', INIT_OFFSET), Value('i', 0)
    cur_time = time.time()
    for pname in ['PROC-' + str(i) for i in range(1, PROC_NUM + 1)]:
        proc = Process(target=worker, args=(offset, action_count, pname))
        processes.append(proc)
        proc.start()

    for proc in processes:
        proc.join()

    # Done! Print out the results
    event_count = offset.value + action_count.value - 1
    time_pass = time.time() - cur_time
    print('----------Done!----------')
    print('%d users created, %d actions simulated.'
          %(offset.value - 1, action_count.value))
    print('Total: %d events in %f sec (%f events/sec)'
          %(event_count, time_pass, event_count / time_pass))

if __name__ == '__main__':
    main()
