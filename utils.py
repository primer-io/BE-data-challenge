"""
This script implements util functions for Primer Data Engineering task

"""
LOG_TEMPLATE = "Primer Task - {0}"

def nested_loop_join(left, right, key):
    """
    Implements a simple nested loop join between two dictionaries assuming that both structures share the same key name
    """
    data = []
    if left and right and key:
        for l in left:
            joined = []
            for r in right:
                if l[key] == r[key]:
                    l_copy = l.copy()
                    l_copy.update(r)
                    joined.append(l_copy)
            if len(joined) > 0:
                data = data + joined
    return data


def merge_join(left, right, key):
    """
    Implements a sort-merge join between two dictionaries assuming that both structures share the same key name
    """
    if left and right and key:
        inner = sorted(left, key=lambda d: d[key])
        outer = sorted(right, key=lambda d: d[key])

        i = 0
        o = 0

        while i < len(inner) and o < len(outer):
            k = min(inner[i][key], outer[o][key])

            inner_group = []
            outer_group = []

            # Addesses cases where there are multiple rows on the left and right for the same key
            while (i < len(inner) and k == inner[i][key]):
                inner_group.append(inner[i])
                i += 1

            while (o < len(outer) and k == outer[o][key]):
                outer_group.append(outer[o])
                o += 1

            for ig in inner_group:
                for og in outer_group:
                    yield ig | og

def hash_join(left, right, key):
    """
    Implements a hash join between two dictionaries assuming that both structures share the same key name
    """
    if left and right and key:
        hash_table = {}
        res = []
        for v in right:
            hash_table[v[key]] = hash_table.get(v[key], []) + [v]

        for l in left:
            if l[key] in hash_table:
                for v in hash_table[l[key]]:
                    yield l | v 


def flatten_dict(d):
    """
    Implements dict flattening 
    """
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        else:
            out[name[:-1]] = x
    flatten(d)
    return out

def log(message):
    print(LOG_TEMPLATE.format(message))