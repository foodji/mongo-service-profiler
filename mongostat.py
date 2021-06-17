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
from   bson.son  import SON
import argparse

DB_TEST_NAME='test'

CHOICES=["app", "op", "command"]


class MapCodes:

    # TODO: External files can be used to store these functions and loaded on 
    # demand

    @staticmethod
    def map_command():
        """
        TODO: Docstring for map_command.
        :returns: TODO
        """
        # return Code("""function(){ 
                # emit(this.appName, this.command) 
        # }""")
        return Code("""function(){
                let commcopy = Object.assign({}, this.command);
                if(commcopy["q"] != undefined)
                {
                    commcopy["q"]["_id"] = commcopy["q"]["_id"].valueOf();                
                }
                if(commcopy["filter"] != undefined )
                {
                    commcopy["filter"]["id"] = commcopy["filter"]["id"].valueOf();                
                }
                if(commcopy["lsid"] != undefined)
                {
                    commcopy["lsid"]["id"] = commcopy["lsid"]["id"].toString().split('"')[1];                
                }
                emit( this.appName, commcopy  );
        }""")

    @staticmethod
    def reduce_command():
        """
        TODO: Docstring for map_command.
        :returns: TODO
        """
        # return Code("function(key, values){ return JSON.stringify(values) }")
        return Code("function(k,v){return v;}")

    @staticmethod
    def finalize_command():
        return Code("function(key,red){return red;}")


class DBActor:
    """A Database actor will perform a set of operations against a connection"""

    def __init__(self, name, address, useSSl=False):
        """
        """
        url = "mongodb://{}:27017/?readPreference=primary&appname={}&ssl={}".format(
                address, name, str(useSSl).lower()
                )
        self._conn  = Connection(url)
        self._db    = self._conn[DB_TEST_NAME]
        self._idlist = self._db.post.distinct("_id", {})

    def setProfilingLevel(self):
        self._db.set_profiling_level(2)

    def write(self):
        """
        Creates a post with random data:w
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
        Creates a post with random data:w
        """
        upid = random.choice(self._idlist)
        post = {
                "text"  : ''.join(random.choices(string.ascii_letters + string.digits, k=54)),
                }
        self._db.post.update_one({"_id" : upid}, { "$set": post })

    def read(self):
        """
        Read an entry
        """
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
    """A runtime that spawns and runs actor transactions random"""
    def __init__(self, ticks=200, host="localhost"):
        self._readActor   = DBActor("ReadActor",   host)
        self._writeActor  = DBActor("WriteActor",  host)
        self._updateActor = DBActor("UpdateActor", host)
        self._deleteActor = DBActor("DeleteActor", host)
        self._hybridActor = DBActor("HybridActor", host)
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


class Aggregator:

    """Collects aggregate information"""

    # TODO: Command data

    def __init__(self, address='localhost', useSSl=False):
        """
        Constructor
        """
        url = "mongodb://{}:27017/?readPreference=primary&appname=Aggregator&ssl={}".format(
                address, str(useSSl).lower()
                )
        self._conn  = Connection(url)
        self._db    = self._conn[DB_TEST_NAME]


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
                out  : "command_maps"
            }
        )
        """
        data = self._db.get_collection("system.profile").map_reduce(
                MapCodes.map_command(),
                MapCodes.reduce_command(),
                # "appname_commands",  ## TODO: Make optional not hardcoded
                out=SON([('inline',1)]),
                finalize=MapCodes.finalize_command()
                )
        pprint(data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mongostat - Collect MongoDB statistics')
    # parser.add_argument("echo", help="echo the string you use here")
    parser.add_argument("--gen", help="Generate [GEN] Test database entries",
            type=int) # hidden variable set to true, no extra args required
    parser.add_argument("--agg", help="Aggregate output", choices=CHOICES)
    args = parser.parse_args()
    if args.gen and args.gen > 0:
        print("Generating {} Entries into ` {} ` Database".format(args.gen,
            DB_TEST_NAME))
        print("=======================")
        CRUDRuntime().run()
        print("=======================")
    else:
        if args.gen:
            print("Expected positive number of entries for Test Data generation")

    if args.agg:
        agg = Aggregator()
        print("Running {} aggregation".format(args.agg))
        print("=======================")
        if args.agg == "op":
            agg.group_by_op()
        elif args.agg=="app":
            agg.group_by_app()
        else:
            agg.group_by_command()
        print("=======================")
