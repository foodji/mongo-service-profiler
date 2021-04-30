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
from   pymongo  import MongoClient as Connection

DB_TEST_NAME='test'

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
            "tags"  : ["mongodb", "python", "pymongo"],
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


if __name__ == '__main__':
    print("Mongostat db generator")
    print("Running 200 actions")
    print("=======================")
    CRUDRuntime().run()
    print("\tDone")
    print("=======================")
