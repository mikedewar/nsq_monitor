import requests
import time
import redis

r = redis.StrictRedis(db=0)
carddb = redis.StrictRedis(db=4)

def get_topics(lookupd="http://127.0.0.1:4161"):
    url = lookupd+"/topics"
    r = requests.get(url)
    topics =  r.json()['data']['topics']
    topics.sort()
    return topics

def get_data_dict(topic):
    data = r.zrevrange(topic, 0, -1, withscores=True)
    keys, counts = zip(*data)
    print counts
    percents = [round(100 * c / float(max(counts)),2) for c in counts]
    boundeds = [carddb.hget(topic+key, 'bounded') for key in keys]
    types =  [carddb.hget(topic+key, 'type') for key in keys]
    return zip(keys, percents, boundeds, types)


def get_boundedness(topic):
    return [
        (key, carddb.hget(topic+key, 'bounded')) 
        for key in r.zrevrange(topic, 0, -1)
    ]

def flatten_keys(d,parent=None):
    assert isinstance(d,dict), (d,parent)
    out = set()
    for key,value in d.items():
        new_parent = '.'.join([parent,key]) if parent else key
        if isinstance(value,dict):
            for k in flatten_keys(value, new_parent):
                out.add(k)
        if isinstance(value,list):
            new_parent += ".[]"
            for di in value:
                if isinstance(di, dict):
                    for k in flatten_keys(di, new_parent):
                        out.add(k)
        else:
            out.add(new_parent)
    return out

def flatten(d,parent=None):
    assert isinstance(d,dict), (d,parent)
    out = set()
    for key,value in d.items():
        new_parent = '.'.join([parent,key]) if parent else key
        if isinstance(value,dict):
            for k,v in flatten(value, new_parent):
                out.add((k,v))
        elif isinstance(value,list):
            new_parent += ".[]"
            for di in value:
                if isinstance(di, dict):
                    for k,v in flatten(di, new_parent):
                        out.add((k,v))
        else:
            out.add((new_parent,value))
    return out

def is_numeric(l):
    # if we can cast everything to a float without raising an error
    for li in l:
        try:
            float(li)
        except ValueError:
            return False
    return True

def is_homogenous(types):
    # if we have a heterogenous list, then return False
    if all([t == types[0] for t in types]):
        return True
    return False

def is_int(l):
    # if the list is numeric, then continue
    if is_numeric(l):
        l = [float(li) for li in l]
    else:
        return False
    types = [type(li) for li in l]
    # if we already have a list of ints then return True
    if all([t is int for t in types]):
        return True
    # if casting to int doesn't change the contents then return True
    if all([int(li) == li for li in l]):
        return True

def is_float(l):
    if not is_numeric(l):
        return False
    if is_int(l):
        return False
    return True

def what_type(l):
    if is_int(l):
        return 'int'
    if is_float(l):
        return 'float'
    return 'str'

def is_bounded(l, threshold=0.1):
    if sum(l) / float(len(l)) < threshold:
        return True
    return False

def call_it(key, topic, value, setsdb, carddb, ratedb, typesdb):

    r.zincrby(topic, key)
    setsdb.sadd(topic+key,value)

    ct = setsdb.scard(topic+key)
    # default is a one second bin.. 
    oldt = carddb.hget(topic+key,'t') or time.time() - 1
    oldct = carddb.hget(topic+key,'ct') or 0
    
    # let's just sort out types for a moment 
    oldt = float(oldt)
    oldct = int(oldct)
    ct = int(ct)
    t = time.time()

    # store state
    carddb.hset(topic+key, 't', t)
    carddb.hset(topic+key, 'ct', ct)

    # calcualte rate
    rate = ct / float(r.zscore(topic, key))
    #print key, rate, ct, oldct
    carddb.hset(topic+key, 'rate_t', rate)
    ratedb.lpush(topic+key, rate)

    # calculate boundedness
    carddb.hset(
        topic+key, 
        'bounded', 
        is_bounded(map(float, ratedb.lrange(topic+key, 0, int(ct*.1))))
    )

    # infer type
    ty = what_type(setsdb.smembers(topic+key))
    carddb.hset(topic+key, 'type', ty)

if __name__ == "__main__":
    a = {"a":[{"b":3},{"b":4}], "c":3}
    print flatten(a)

