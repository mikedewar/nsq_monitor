import nsq
import logging
import argparse
import json
import redis

import utils

parser = argparse.ArgumentParser(description='NSQ reader to monitor messages in a topic')

parser.add_argument(
    '-p', '--port',
    help="port of redis server",
    default=6379
)
parser.add_argument(
    '-r', '--redishost',
    help="host of redis server",
    default="localhost"
)

parser.add_argument(
    '-q', '--percent',
    help="percentage of messages to consume (only makes sense if used with /mput)",
    default="100"
)

parser.add_argument(
    '-t', '--topic',
    required=True,
    help="NSQ Topic you'd like to monitor"
)
parser.add_argument(
    '-l', '--lookupd',
    help="http address of lookupd",
    default='http://127.0.0.1:4161'
)

args = parser.parse_args()

rdb = redis.StrictRedis(host=args.redishost, port=args.port, db=0)

counter = 0

def monitor(message):
    global counter

    counter += 1
    if counter > 100:
        counter = 0
    if counter > args.percent:
        return True
    
    try:
        msg = message.body.strip().strip('"').decode('string-escape')
        msg = json.loads(msg)
    except ValueError:
        print "FAIL"
        print message.body.strip().strip('"').decode('string-escape')
        raise


    return True

    for key in utils.flatten_keys(msg):
        rdb.zincrby(args.topic, key)

tasks = {"monitor": monitor}

r = nsq.Reader(
    tasks,
    lookupd_http_addresses=[args.lookupd],
    topic=args.topic,
    channel="%s_monitor"%args.topic,
    max_in_flight = 100,
)
nsq.run()
