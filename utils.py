import requests
import time
import redis

r = redis.StrictRedis(db=0)

def get_topics(lookupd="http://127.0.0.1:4161"):
    url = lookupd+"/topics"
    r = requests.get(url)
    topics =  r.json()['data']['topics']
    topics.sort()
    return topics

def get_data_dict(topic):
    return r.zrevrange(topic, 0, -1, withscores=True)

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
            for k,v in flatten_keys(value, new_parent):
                out.add((k,v))
        if isinstance(value,list):
            new_parent += ".[]"
            for di in value:
                if isinstance(di, dict):
                    for k,v in flatten_keys(di, new_parent):
                        out.add(k,v)
        else:
            out.add(new_parent,value)
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

def call_it(key,setsdb, carddb, ratedb, typesdb):
    ct = setsdb.scard(key)
    oldt = carddb.hget(key,'oldt')
    oldct = carddb.hget(key,'oldct')
    t = time.time()
    rate = (ct - oldct) / float((t - oldt))
    ratedb.lpush(key,rate)
    if ratedb.llen(key) > 100:
        # look at the last 10
        rates = ratedb.lrange(key, 0, 10)
        # if the average rate of increase is less than one
        # then we assume that this set is converging in cardinality
        v = setsdb.smembers(key)
        if sum(rates)/len(rates) < 1:
            typesdb.set(key, 'bounded'+what_type(v))
        else:
            typesdb.set(key, 'unbounded'+what_type(v))

if __name__ == "__main__":
    a = {"a":[{"b":3},{"b":4}], "c":3}
    print flatten(a)

