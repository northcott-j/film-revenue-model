import pymongo
from Actor import Actor
from Film import Film
from config.GLOBALS import MONGO_URL, MONGO_DB
from Queue import Queue


def import_actors(db):
    """
    Imports Actors, creates object and adds to queue
    :param db: db connection
    :return: queue of actors
    """
    actors = db['actors'].find({})
    return_q = Queue()
    for a in actors:
        created_a = Actor(a['id'], a['DIRECTOR'])
        created_a.import_fields(a)
        return_q.put(created_a)
    return return_q


def import_films(db):
    """
    Imports Films, creates object and adds to queue
    :param db: db connection
    :return: queue of films
    """
    films = db['films'].find({})
    return_q = Queue()
    for f in films:
        # I didn't save mojo_year so using a random number
        # It isn't important as these films were already saved
        created_f = Film(f['mojo_id'], f['mojo_title'], 2017)
        created_f.import_fields(f)
        return_q.put(created_f)
    return return_q


def main():
    print "Connecting to {0}...".format(MONGO_URL)
    client = pymongo.MongoClient(MONGO_URL)
    print "Linking to the following data: {0}...".format(MONGO_DB)
    db_conn = client[MONGO_DB]
    # Add all finished films to the Film.dict
    film_output_q = import_films(db_conn)
    actor_output_q = import_actors(db_conn)
    while not film_output_q.empty():
        q_film = film_output_q.get()
        film_output_q.task_done()
        Film.all_films[q_film.id] = q_film

    # Add all finished actors to the Actor.dict
    while not actor_output_q.empty():
        a_film = actor_output_q.get()
        actor_output_q.task_done()
        if a_film.DIRECTOR:
            Actor.all_actors["director-{0}".format(a_film.id)] = a_film
        else:
            Actor.all_actors[a_film.id] = a_film

    # Call aggregate on every film
    for f_id in Film.all_films:
        Film.all_films[f_id].set_aggregate_fields()

    # Save all of the films to MongoDb
    for f_id in Film.all_films:
        db_conn['films_agg'].insert(Film.all_films[f_id].export())
