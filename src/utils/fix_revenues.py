""" Some revenues are missing because I only looked for Worldwide revenue. This will fill in 0's with domestic """
import pymongo
from data_collection.Film import Film
from config.GLOBALS import MONGO_DB, MONGO_URL


def main():
    print "Connecting to {0}...".format(MONGO_URL)
    client = pymongo.MongoClient(MONGO_URL)
    print "Linking to the following data: {0}...".format(MONGO_DB)
    db_conn = client[MONGO_DB]
    # Films that need to be fixed
    fix_films = db_conn['films'].find({"revenue": 0, "FAILED": False})
    for ff in fix_films:
        # Make a Film class (only mojo_id is important)
        tmp_f = Film(ff['mojo_id'], 'NoTitle', 3000)
        new_rev = tmp_f.set_revenue()
        # If the revenue is greater than zero, update the revenue field
        if new_rev > 0:
            print "Updating the revenue for Film:{0} to ${1}".format(ff['mojo_id'], str(new_rev))
            db_conn['films'].update({"_id": ff["_id"]}, {'$set': {'revenue': new_rev}})
