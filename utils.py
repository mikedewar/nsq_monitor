def get_topics():
    return ["bob", "alice", "drew"]

def flatten_keys(d,parent=None):
    out = set()
    for key,value in d.items():
        new_parent = '.'.join([parent,key]) if parent else key
        if isinstance(value,dict):
            for k in flatten_keys(value, new_parent):
                out.add(k)
        if isinstance(value,list):
            new_parent += ".[]"
            for di in value:
                for k in flatten_keys(di, new_parent):
                    out.add(k)
        else:
            out.add(new_parent)
    return out
