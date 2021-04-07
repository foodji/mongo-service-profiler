################################################################################
#                    MongoStat.py - Collect profiler stats                     #
################################################################################
#######################
#  Author: Harry K    #
#  Date  : 2021-04-07 #
#######################


# Adapted from: https://gist.github.com/sergray/1878413
# https://docs.mongodb.com/manual/reference/connection-string/index.html#dns-seed-list-connection-format

"""
Script for automated analysis of profiling data in MongoDB,
gathered by Mongo with db.setProfilingLevel(1).
See <http://www.mongodb.org/display/DOCS/Database+Profiler>
TODO: pass collection and database with profiling data in arguments
TODO: make thread-safe
TODO: handle map-reduce operations
"""
from collections import defaultdict
from pprint import pprint


MONGO_DB = 'test'
PROFILE_COLLECTION = 'system.profile'  # default name of collection with profiling data

# global mapping of (collection, query_fields) to their statistics data
QSTATS = defaultdict(lambda: {
    'count': 0, 'millis_sum': 0, 'millis_min': None, 'millis_max': None,
    'nscanned_sum': 0, 'nscanned_min': None, 'nscanned_max': None
})


def get_profile_collection():
    """Return mongo collection containing profiling records"""
    from pymongo import MongoClient as Connection

    _URL = "mongodb://192.168.43.207:27017/?readPreference=primary&appname=Script&ssl=false"
    # URL = "mongodb+srv://harryk:sysadminharry13807@192.168.43.207/admin?retryWrites=true&w=majority"
    con = Connection(_URL)
    print("Connection found: =>   {}".format(con))
    # db = con[MONGO_DB]
    db = con.test
    # col = db[PROFILE_COLLECTION]
    print("Current profiling level: => {}", db.profiling_level())
    print("Current profiling info : => {}", db.profiling_info())
    print("Available collections")
    pprint(db.list_collection_names())
    mcol = db.get_collection(PROFILE_COLLECTION)
    return mcol


def extract_collection_query(prof_rec):
    """Returns tuple of collection name and list of query fields"""
    ns = prof_rec[u'ns']
    if ns.endswith(u'$cmd'):
        cmd_info = prof_rec[u'command']
        qry_fields = extract_fields(cmd_info.pop(u'query', {}))
        fields = cmd_info.pop(u'fields')
        command, collection = cmd_info.popitem()
    else:
        collection = ns.rsplit(u'.').pop()
        query = prof_rec[u'query']
        if u'$query' in query:
            qry_fields = extract_fields(query[u'$query'])
        else:
            qry_fields = extract_fields(query)
        if u'$orderby' in query:
            ord_fields = [f + [u'$orderby'] for f in extract_fields(query[u'$orderby'])]
            qry_fields.extend(ord_fields)
    return (collection, [u'.'.join(f) for f in qry_fields])


def extract_fields(query, parent_fields=None):
    """Recursively descend query prototype and return list of field names"""
    fields = []
    if not parent_fields:
        parent_fields = []
    # field_path = lambda k: '.'.join(parent_fields + [k])
    for k,v in query.items():
        if isinstance(v, dict):
            fields.extend(extract_fields(v, parent_fields + [k]))
        else:
            fields.append(parent_fields + [k])
    return fields


def _update_stats(col, qry_fields, prof_rec):
    stat_key = (col, tuple(qry_fields))
    stats = QSTATS[stat_key]
    stats['count'] += 1
    millis = prof_rec.get(u'millis')
    if millis:
        stats['millis_sum'] += millis
        if stats['millis_min'] is None or stats['millis_min'] > millis:
            stats['millis_min'] = millis
        if stats['millis_max'] is None or stats['millis_max'] < millis:
            stats['millis_max'] = millis
    nscanned = prof_rec.get(u'nscanned')
    if nscanned:
        stats['nscanned_sum'] += nscanned
        if stats['nscanned_min'] is None or stats['nscanned_min'] > nscanned:
            stats['nscanned_min'] = nscanned
        if stats['nscanned_max'] is None or stats['nscanned_max'] < nscanned:
            stats['nscanned_max'] = nscanned


def show_stats():
    for (col, fields), stats in QSTATS.items():
        print([col, fields])
        info = stats.copy()
        if info['count']:
            if info['millis_sum'] is not None:
                info['avg_millis'] = info['millis_sum'] / info['count']
            else:
                info['avg_millis'] = None
            if info['nscanned_sum'] is not None:
                info['avg_nscanned'] = info['nscanned_sum'] / info['count']
            else:
                info['avg_nscanned'] = None
        print("count=%(count)d avg_millis=%(avg_millis)r avg_nscanned=%(avg_nscanned)r" % info)


def analyze_profiling_data():
    """Process all records in profiling collection and gather statistics"""
    prof_col = get_profile_collection()
    for rec in prof_col.find():
        try:
            col, qry_fields = extract_collection_query(rec)
        except:
            # quick workaround, needs better handling
            continue
        _update_stats(col, qry_fields, rec)


if __name__ == '__main__':
    analyze_profiling_data()
    show_stats()
