import nsq
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
setsdb = redis.StrictRedis(host=args.redishost, port=args.port, db=1)
ratedb = redis.StrictRedis(host=args.redishost, port=args.port, db=2)
typesdb = redis.StrictRedis(host=args.redishost, port=args.port, db=3)
carddb = redis.StrictRedis(host=args.redishost, port=args.port, db=4)

counter = 0
percent = float(args.percent)

def monitor(message):
    global counter

    counter += 0.01
    if counter > 100:
        counter = 0
    if counter > percent:
        return True
    print "processing"
    
    msg = message.body
    try:
        msg = json.loads(msg)
    except ValueError:
        print msg
        print "FAIL"
        raise

    for key,value in utils.flatten(msg):
        utils.call_it(key, args.topic, value, setsdb, carddb, ratedb, typesdb)
    return True

tasks = {"monitor": monitor}

r = nsq.Reader(
    tasks,
    lookupd_http_addresses=[args.lookupd],
    topic=args.topic,
    channel="%s_monitor"%args.topic,
    max_in_flight = 2000,
)
nsq.run()
