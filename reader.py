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

def monitor(message):
    try:
        blob = json.loads(message.body)
    except ValueError:
        print "FAIL"
        raise
    assert isinstance(blob,dict), blob
    for key in utils.flatten_keys(blob):
        logging.info(key)
        print key
        rdb.zincrby(args.topic, key)
    return True

tasks = {"monitor": monitor}

r = nsq.Reader(
    tasks,
    lookupd_http_addresses=[args.lookupd],
    topic=args.topic,
    channel="%s_monitor"%args.topic
)
nsq.run()
