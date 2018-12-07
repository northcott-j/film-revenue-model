import data_collection.Film
import data_collection.Actor
import pymongo
from config.GLOBALS import MONGO_URL, MONGO_DB


f = data_collection.Film.Film('starwars', 'Star Wars: Episode I - The Phantom Menace', '1999')
print f.set_imdb_id()
print f.set_release_date()

"""
a = data_collection.Actor.Actor('nm1132744', False)
p = a.set_imdb_page()
a.set_non_aggregate_fields()



def drop_collections(db):
    from config.GLOBALS import COLLECTIONS
    print "Dropping current MongoDB collections..."
    cols = db.collection_names()
    for c in COLLECTIONS:
        if c in cols:
            print "Dropping {0}...".format(c)
            db.drop_collection(c)
import pymongo
from config.GLOBALS import MONGO_URL, MONGO_DB
print "Connecting to {0}...".format(MONGO_URL)
client = pymongo.MongoClient(MONGO_URL)
print "Linking to the following data: {0}...".format(MONGO_DB)
db_conn = client[MONGO_DB]
drop_collections(db_conn)
db_conn['actors'].insert(a.export())
db_conn['films'].insert(f.export())


import data_collection.SetQueue
s = data_collection.SetQueue.SetQueue()
for i in range(0, 1000):
    s.put('seen_me')
print s.qsize()


import data_collection.SetQueue
print "Connecting to {0}...".format(MONGO_URL)
client = pymongo.MongoClient(MONGO_URL)
print "Linking to the following data: {0}...".format(MONGO_DB)
db_conn = client[MONGO_DB]
films = db_conn['films'].find({})
unique_people = set()
peoples = []
for f in films:
    print f
    director = "director-{0}".format(f.get('director', ''))
    if director not in unique_people:
        peoples.append({'id': f.get('director', ''), 'DIRECTOR': True})
        unique_people.add(director)
    for a in f.get('actors', []):
        if a not in unique_people:
            peoples.append({'id': a, 'DIRECTOR': False})
            unique_people.add(a)
print peoples
"""