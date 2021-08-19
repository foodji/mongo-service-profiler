################################################################################
#                    MongoStat.py - Collect profiler stats                     #
#                        #######################################################
#  Modified : Harry K    #
#  Date     : 2021-04-07 #
##########################


"""
Script for automated analysis of profiling data in MongoDB,
gathered by Mongo with db.setProfilingLevel(1).
See <http://www.mongodb.org/display/DOCS/Database+Profiler>
https://studio3t.com/knowledge-base/articles/mongodb-query-performance/
"""
import string
import random
import datetime
from pprint import pprint
from pymongo import MongoClient as Connection, ALL
from bson.code import Code
# from   bson.son  import SON
import argparse

DB_TEST_NAME = 'test'

CHOICES = ["app", "op", "command"]


class MapCodes:

    # TODO: Different versions may be producing different output structures, we
    # may need to load Code data based on mongo version information

    @staticmethod
    def scrubber():
        """
        Global variables and functions
        This method converts some specific problematic BSON objects into plain
        strings so that the JSON.stringify and JSON.parse work as intended. It
        should probably be broken up to be more granular
        """
        return Code("""
                function(obj, astage, bstage, tov)
                {
                    if(obj[astage] !== undefined)
                    {
                        if(obj[astage][bstage] !== undefined)
                        {
                            if(tov)
                            {
                               obj[astage][bstage] = obj[astage][bstage].valueOf();
                            } else
                            {
                               obj[astage][bstage] = obj[astage][bstage].valueOf().toString().split('"')[1];
                            }
                        }
                    }
                }
        """)

    @staticmethod
    def stripcontext():
        # TODO: implement a 'filter' scheme for this
        """
        Strip unnedded context in the command info
        Converts some command specific context into constant values that will
        be filtered out

        NB: Only the filter key is targeted since thats where the 'query'
        context lives. Other keys such as lsid ,limit, singleBatch or cursors
        arent targeted and may affect the output

        TODO: The vocabulary of values mapped to true is a large one and needs
        to be expanded , perhaps recursively
        TODO: Better handle 'r' closure for bucketId
              query -> bucketId -> $in : [ .. ids]
        """
        return Code("""
            function(command)
            {
                [
                  { k: "filter", c: (v) => true, r: (v) => true},
                  { k: "lsid", c: (v) => ["id"].some((i) => v==i), r: (v) => true},
                  { k: "query", c: (v) => ["id"].some((i) => v==i), r: (v) => true },
                  { k: "query", c: (v) => ["bucketSetId"].some((i) => v==i), r: (v) => true},
                  { k: "query", c: (v) => ["accountId"].some((i) => v==i), r: (v) => true},
                  { k: "query", c: (v) => ["bucketId"].some((i) => v==i),
                    r: (v) =>  Object.keys(v).map((i) => {
                        let o = {}; o[i] = true; return o;
                    })},
                  { k: "q", c: (v) => ["_id"].some((i) => v==i), r: (v) => true},
                  { k: "q", c: (v) => ["id"].some((i) => v==i), r: (v) => true},
                  { k: "u", c: (v) => ["$set"].some((i) => v==i), r: (v) => true},
                ].reduce(function(acc, curr){
                    if(acc[curr.k])
                    {
                        // if the accumulator has the key k, filter by
                        // c, then map filtered entries to True
                        Object.keys(acc[curr.k]).filter(curr.c).map((k) => {
                            acc[curr.k][k] = curr.r(acc[curr.k][k]);
                        });
                    }
                    return acc;
                }, command);
            }
        """)

    @staticmethod
    def map_command():
        """
        Map stage of the MapReduce, does some scrubbing and stripping
        TODO: At this stage, compute the statistics and pass them as value in
              the final output object.
        """
        return Code("""
            function(){
                let commcopy = Object.assign({}, this.command);
                scrubber(commcopy, "q", "_id", true);
                scrubber(commcopy, "filter", "id", true);
                scrubber(commcopy, "lsid", "id", false);
                stripcontext(commcopy);
                emit( this.appName, commcopy  );
            }""")

    @staticmethod
    def reduce_command():
        """
        Reduce stage of the MapReduce. Stringifies the value into a Set, to
        retain unique entries

        NB: On the totaling approach used (hashtable)
        A better way would be to sort the incoming values and compare the
        neighbouring values - but that is compute heavy
        This is simpler but requires more memory
        """
        # return Code("function(key, values){ return JSON.stringify(values) }")
        # TODO: Decouple totaling function and abstract to other possible ways
        return Code("""
            // Some older versions of mongo require a values key in the object
            // Use a hashtable to store sums,
            function(k,v){
                let itemset = v.reduce((acc, curr) => {
                    let jsonstr = JSON.stringify(curr);
                    if(acc.hashtable[jsonstr] !== undefined) {
                        acc.hashtable[jsonstr] = acc.hashtable[jsonstr] + 1;
                    } else {
                        acc.hashtable[jsonstr] = 1;
                    }
                    acc.items.add(jsonstr);
                    return acc;
                }, { items: new Set(), hashtable: {} })
                return {
                    values: Array.from(itemset.items),
                    totals: itemset.hashtable
                };
            }
        """)

    @staticmethod
    def finalize_command():
        """
        Finalize stage after the mapreduce to reconvert back the stringified
        objects and embedding the computed function i.e total for this case
        """
        return Code("""
            function(key, val)
            {
                let itemdata = val.values.reduce((acc, curr) => {
                    let jsondata = JSON.parse(curr);
                    jsondata["census"] = val.totals[curr]
                    acc.push(jsondata);
                    return acc;
                }, []);
                itemdata.sort((v,b) => v.census > b.census);
                return itemdata;
            }""")


class DBActor:
    """
    A Database actor will perform a set of operations against a connection
    """

    def __init__(self, name, url, table='test'):
        """
        Start a DBActor
        """
        # url += ("&" if "?" in url else "/?") + "appname={}".format(name)
        # Pass appname parameter within connection constructor
        self._conn = Connection(url, appname=name)
        self._db = self._conn[table]
        self._idlist = self._db.post.distinct("_id", {})

    def setProfilingLevel(self):
        """
        Set profiling level for events
        """
        self._db.set_profiling_level(level=ALL)

    def count(self):
        """
        Count operation
        """
        self._db.post.count_documents(filter={})

    def write(self):
        """
        Creates a post with random data
        """
        post = {
                "author": ''.join(
                    random.choices(string.ascii_letters + string.digits, k=26)),
                "text": ''.join(
                    random.choices(string.ascii_letters + string.digits, k=54)),
                "tags": ["mongodb", "python", "pymongo"],
                "date": datetime.datetime.utcnow()
                }
        uid = self._db.post.insert_one(post).inserted_id
        self._idlist.append(uid)

    def update(self):
        """
        Creates a post with random data
        """
        if len(self._idlist) > 0:
            upid = random.choice(self._idlist)
            post = {
                    "text": ''.join(
                        random.choices(
                            string.ascii_letters + string.digits,
                            k=54)),
                    }
            self._db.post.update_one({"_id": upid}, {"$set": post})

    def read(self):
        """
        Read an entry
        """
        if len(self._idlist) > 0:
            upid = random.choice(self._idlist)
            self._db.post.find_one({"id": upid})

    def delete(self):
        """
        Delete an entry
        """
        if len(self._idlist) > 0:
            upid = random.choice(self._idlist)
            self._db.post.delete_one({"_id": upid})
            self._idlist.remove(upid)


class CRUDRuntime:
    """A runtime that spawns and runs actor transactions randomly
       Use to generate test entries!
    """
    def __init__(self, ticks=200, url='mongodb://127.0.0.1:27017', table='test'):
        self._readActor   = DBActor("ReadActor",   url, table=table)
        self._writeActor  = DBActor("WriteActor",  url, table=table)
        self._updateActor = DBActor("UpdateActor", url, table=table)
        self._deleteActor = DBActor("DeleteActor", url, table=table)
        self._hybridActor = DBActor("HybridActor", url, table=table)
        self._counter = 0
        self._ticks = ticks

    def run(self):
        """
        RUn the simulation
        """
        _actions = range(0, 4)
        # Some parameters for profiling can be passed here
        self._hybridActor.setProfilingLevel()
        while self._counter < self._ticks:
            _choice = random.choice(_actions)
            if _choice == 0:
                self._readActor.read()
            elif _choice == 1:
                self._writeActor.write()
            elif _choice == 2:
                self._deleteActor.delete()
            else:
                self._updateActor.update()
            if random.randint(0, 7) > random.randint(0, 7):
                self._hybridActor.count()
            elif _choice%2 == 0 :
                self._hybridActor.write()
            self._counter = self._counter + 1
        print("Done")


class Aggregator:

    """Collects aggregate information"""

    # TODO: Tweak all options to either Write to a collection or output

    def __init__(self, url='mongo://127.0.0.1:27017', db='test',
            collection='system.profile'):
        """
        Constructor
        """
        # url += ("&" if "?" in url else "?") +
        # "appname={}".format("Mongostat Aggregator")
        self._conn = Connection(url, appname="Mongostat Aggregator")
        self._db = self._conn[db]
        self._coll = collection
        info = self._conn.server_info()
        dbi = self._db.profiling_level()
        print("-----------------------------------------")
        print("| Mongo version: {} +git {}\n| Profiling level: {} ".format(
            info['version'],
            info['gitVersion'], dbi))
        print("-----------------------------------------")

    def group_by_app(self):
        """
        db.getCollection("system.profile").aggregate([
            { $group : {
                _id   : { appName : '$appName', op  :'$op' },
                    total : {  $sum : 1 }
                }
            },
            { $project : {
                    appName : '$_id.appName',
                    op      : '$_id.op',
                    total   : '$total' ,
                    _id     : 0
                }
            },
            { $sort : { "appName" : 1 } }
        ]);
        """
        data = self._db.get_collection(self._coll).aggregate([
            {
                '$group': {
                    '_id': {'appName': '$appName', 'op': '$op'},
                    'total': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'appName': '$_id.appName',
                    'op': '$_id.op',
                    'total': '$total',
                    '_id': 0
                    }
                },
            {'$sort': {"appName": 1}}
            ])
        pprint(list(data))

    def group_by_op(self):
        """
        db.getCollection("system.profile").aggregate([
            { $group   : {
                _id : "$op",
                apps: { $addToSet : "$appName" },
                total: { $sum : 1 }
                }
            },
        ])
        """
        data = self._db.get_collection(self._coll).aggregate([
            {
                '$group':
                {
                    '_id': "$op",
                    'apps': {
                        '$addToSet': "$appName"
                    },
                    'total': {'$sum':  1}
                }
            },
        ])
        pprint(list(data))

    def group_by_command_commit(self, output):
        """
        db.system.profile.mapReduce(
            function()
            {
                emit( this.appName, this.command  );
            },
            function( key, values )
            {
                return values;
            },
            {
                query: {},
                out  :  <collection name>,
                finalize: ...
            },
        )
        """
        self._db.get_collection(self._coll).map_reduce(
                map=MapCodes.map_command(),
                reduce=MapCodes.reduce_command(),
                out=output,
                scope={
                    "scrubber": MapCodes.scrubber(),
                    "stripcontext": MapCodes.stripcontext()
                },
                finalize=MapCodes.finalize_command()
            )
        print("--- Output commited into => {} collection".format(output))

    def group_by_command_inline(self):
        """
        db.system.profile.mapReduce(
            function()
            {
                emit( this.appName, this.command  );
            },
            function( key, values )
            {
                return values;
            },
            {
                query: {},
                out  :  { inline:1 },
                finalize: ...
            },
        )
        """
        # TODO: Pass CLI option to select the collection name
        # TODO: Optionally select whether to run inline or writeback
        data = self._db.get_collection(self._coll).inline_map_reduce(
                    map=MapCodes.map_command(),
                    reduce=MapCodes.reduce_command(),
                    scope={
                        "scrubber": MapCodes.scrubber(),
                        "stripcontext": MapCodes.stripcontext()
                    },
                    finalize=MapCodes.finalize_command()
                )
        pprint(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Mongostat - Collect MongoDB statistics')
    # TODO: Add config parameter to configure some arguments in a better way
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
            "--config",
            help="Use Config JSON file containing url, db and collection")
    group.add_argument(
            "--connect",
            help="Connection pair in `url db collection` format (with space)",
            type=str)

    parser.add_argument("--gen", help="Generate [GEN] entries", type=int)
    parser.add_argument("--agg", help="Aggregation", choices=CHOICES)

    outputgroup = parser.add_mutually_exclusive_group()
    outputgroup.add_argument("--inline",
                             help="Inline the output (prints to STDOUT)",
                             action='store_true',
                             default=True)
    outputgroup.add_argument("--commit",
                             help="Writes the output to a collection " +
                             "(command aggregation only)", type=str)

    args = parser.parse_args()

    db = 'test'
    url = 'mongodb://127.0.0.1:27017'
    collection = 'system.profile'

    if args.config:
        import json
        with open(args.config) as fconf:
            jsonconf = json.load(fconf)
            db = jsonconf["db"]
            url = jsonconf["url"]
            collection = jsonconf["collection"]
    else:
        if args.connect:
            connstr = args.connect.split(" ")
            if len(connstr) == 3:
                url = connstr[0]
                db = connstr[1]
                collection = connstr[2]
            else:
                print("URL should be space separated string e.g \"{} {} {}\"".format(url, db, collection))
                print("Using default connection parameters")
        else:
            print("Using default connection parameters")

    print("=========================================")
    print("## URL                : {} ".format(url))
    print("## DB                 : {} ".format(db))
    print("## Profile collection : {} ".format(collection))
    print("=========================================")

    if args.gen and args.gen > 0:
        print("=========================================")
        print("Generating {} Entries into `{}` Database".format(args.gen, db))
        print("=========================================")
        CRUDRuntime(ticks=args.gen, url=url, table=db).run()
        print("=========================================")
    else:
        if args.gen:
            print("Expected positive number of entries" +
                  "for Test Data generation")

    if args.agg:
        agg = Aggregator(url=url, db=db, collection=collection)
        print("=========================================")
        print("Running {} aggregation".format(args.agg))
        print("=========================================")
        if args.agg == "op":
            agg.group_by_op()
        elif args.agg == "app":
            agg.group_by_app()
        else:
            if args.commit:
                if args.commit and len(args.commit) > 0:
                    agg.group_by_command_commit(args.commit)
                else:
                    print("Received invalid commit collection name:" +
                          "{}".format(args.commit))
            else:
                agg.group_by_command_inline()

        print("=========================================")
