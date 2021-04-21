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
TODO: pass collection and database with profiling data in arguments
TODO: make thread-safe
TODO: handle map-reduce operations
"""
import string
import random
import datetime
from pprint import pprint
from pymongo import MongoClient as Connection

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
        self._idmap = {}

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
        ins = self._db.test.insert_one(post).inserted_id
        self._idmap[ins] = {'r' : 0, 'w' : 1, 'u' : 0, 'd' : 0}

    def update(self):
        """
        Creates a post with random data:w
        """
        upid = random.choice(list(self._idmap.keys()))
        post = {
            "text"  : ''.join(random.choices(string.ascii_letters + string.digits, k=54)),
        }
        res = self._db.posts.update_one({"id" : upid}, { "$set": post })
        if res.matched_count == 1 and res.modified_count == 1:
            print("Updated a record")
            self._idmap[upid]['u'] += 1

    def read(self):
        upid = random.choice(list(self._idmap.keys()))
        self._db.posts.find_one({ "id" : upid })
        self._idmap[upid]['r'] += 1
        print("Read a record")


    def delete(self):
        upid = random.choice(list(self._idmap.keys()))
        self._db.posts.find_one({ "id" : upid })
        self._idmap[upid]['r'] += 1
        pass


if __name__ == '__main__':
    import sys
    # TODO: Build random Actors to modify the database so as to generate
    # profiling information
    print(len(sys.argv))
    if len(sys.argv) > 1:
        host = sys.argv[1]
    get_profile_collection()
