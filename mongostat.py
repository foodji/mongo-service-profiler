################################################################################
#                    MongoStat.py - Collect profiler stats                     #
#                        #######################################################
#  Modified : Harry K    #
#  Date     : 2021-04-07 #
##########################


# Adapted from: https://gist.github.com/sergray/1878413
# https://docs.mongodb.com/manual/reference/connection-string/index.html#dns-seed-list-connection-format

"""
Script for automated analysis of profiling data in MongoDB,
gathered by Mongo with db.setProfilingLevel(1).
See <http://www.mongodb.org/display/DOCS/Database+Profiler>
https://studio3t.com/knowledge-base/articles/mongodb-query-performance/
"""
import string
import random
import datetime
from   pprint    import pprint
from   pymongo   import MongoClient as Connection
from   bson.code import Code
# from   bson.son  import SON
import argparse

DB_TEST_NAME='test'

CHOICES=["app", "op", "command"]


class MapCodes:

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
        """
        Strip unnedded context in the command info
        Converts some command specific context into constant values that will be
        filtered out

        NB: Only the filter key is targeted since thats where the 'query'
        context lives. Other keys such as lsid ,limit, singleBatch or cursors arent 
        targeted and may affect the output
        """
        return Code("""
            function(command)
            {
                if(command["filter"])
                {
                    Object.keys(command["filter"]).map((key) => { 
                        command["filter"][key] = true;
                    });
                }
                if(command["lsid"])
                {
                    command["lsid"]["id"] = true;
                }
            }
        """)

    @staticmethod
    def map_command():
        """
        Map stage of the MapReduce, does some scrubbing and stripping
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
        """
        # return Code("function(key, values){ return JSON.stringify(values) }")
        return Code("""
            function(k,v){
                let itemset = new Set();
                v.forEach(function(val){
                    itemset.add(JSON.stringify(val));
                });
                return Array.from(itemset);
            }
        """)

    @staticmethod
    def finalize_command():
        """
        Finalize stage after the mapreduce to reconvert back the stringified
        objects
        """
        return Code("""
            function(key,red)
            {
                let itemset = new Set();
                red.forEach(function(val){
                    itemset.add(JSON.parse(val));
                })
                return Array.from(itemset);
            }""")


class DBActor:
    """A Database actor will perform a set of operations against a connection"""

    def __init__(self, name, url, table='test'):
        """
        """
        url += ("&" if "?" in url else "?") + "appname={}".format(name)
        self._conn  = Connection(url)
        self._db    = self._conn[table]
        self._idlist = self._db.post.distinct("_id", {})

    def setProfilingLevel(self):
        self._db.set_profiling_level(2)

    def write(self):
        """
        Creates a post with random data
        """
        post = {
                "author": ''.join(random.choices(string.ascii_letters + string.digits, k=26)),
                "text"  : ''.join(random.choices(string.ascii_letters + string.digits, k=54)),
                "tags"  : [ "mongodb", "python", "pymongo" ],
                "date"  : datetime.datetime.utcnow()
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
                    "text"  : ''.join(random.choices(string.ascii_letters + string.digits, k=54)),
                    }
            self._db.post.update_one({"_id" : upid}, { "$set": post })

    def read(self):
        """
        Read an entry
        """
        if len(self._idlist) > 0:
            upid = random.choice(self._idlist)
            self._db.post.find_one({ "id" : upid })

    def delete(self):
        """
        Delete an entry
        """
        if len(self._idlist) > 0:
            upid = random.choice(self._idlist)
            self._db.post.delete_one({ "_id" : upid })
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
        _actions = range(0,4)
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
            if random.randint(0,7) > random.randint(0,7):
                self._hybridActor.read()
            elif _choice%2 == 0 :
                self._hybridActor.write()
            self._counter = self._counter + 1
        print("Done")


class Aggregator:

    """Collects aggregate information"""

    def __init__(self, url='mongo://127.0.0.1:27017', db='test'):
        """
        Constructor
        """
        self._conn  = Connection(url)
        self._db    = self._conn[db]


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
        data = self._db.get_collection("system.profile").aggregate([
            { '$group' : {
                '_id'   : { 'appName' : '$appName', 'op'  :'$op' },
                'total' : {  '$sum' : 1 }
                }
                },
            { '$project' : {
                'appName' : '$_id.appName',
                'op'      : '$_id.op',
                'total'   : '$total' ,
                '_id'     : 0
                }
                },
            { '$sort' : { "appName" : 1 } }
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
        data = self._db.get_collection("system.profile").aggregate([ 
            { '$group'   : { 
                '_id'   : "$op", 
                'apps'  : { '$addToSet' : "$appName" },
                'total' : { '$sum'      :  1 }
                } 
                },
            ])
        pprint(list(data))

    def group_by_command(self):
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
        data = self._db.get_collection("system.profile").inline_map_reduce(
                MapCodes.map_command(),
                MapCodes.reduce_command(),
                # "appname_commands",  ## TODO: Make optional not hardcoded
                # out=SON([('inline', 1)]),
                scope={
                    "scrubber" : MapCodes.scrubber(), 
                    "stripcontext": MapCodes.stripcontext() },
                finalize=MapCodes.finalize_command()
                )
        pprint(data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mongostat - Collect MongoDB statistics')
    # parser.add_argument("echo", help="echo the string you use here")
    parser.add_argument("--gen",     help="Generate [GEN] entries",             type=int) 
    parser.add_argument("--agg",     help="Aggregation",   choices=CHOICES)
    parser.add_argument("--db",      help="Database name", default="test",      type=str)
    parser.add_argument("--url",     help="Database url", default="mongodb://127.0.0.1:27017", type=str)
    args = parser.parse_args()

    print("=========================================")
    print("## URL  : {} ".format(args.url))
    print("## DB   : {} ".format(args.db))
    print("=========================================")


    if args.gen and args.gen > 0:
        print("=========================================")
        print("Generating {} Entries into `{}` Database".format(args.gen, args.db))
        print("=========================================")
        CRUDRuntime(ticks=args.gen, url=args.url, table=args.db).run()
        print("=========================================")
    else:
        if args.gen:
            print("Expected positive number of entries for Test Data generation")

    if args.agg:
        agg = Aggregator(url=args.url, db=args.db)
        print("=========================================")
        print("Running {} aggregation".format(args.agg))
        print("=========================================")
        if args.agg == "op":
            agg.group_by_op()
        elif args.agg =="app":
            agg.group_by_app()
        else:
            agg.group_by_command()
        print("=========================================")
