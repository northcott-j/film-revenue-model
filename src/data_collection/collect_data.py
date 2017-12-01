""" Script to start data collection process """
import pymongo
import requests
from Actor import Actor
from ActorConsumer import ActorConsumer
from bs4 import BeautifulSoup
from datetime import datetime
from Film import Film
from FindIMDbConsumer import FindIMDbConsumer
from Queue import Queue
from ScrapeIMDbConsumer import ScrapeIMDbConsumer
from SetQueue import SetQueue
from time import sleep
from src.config.GLOBALS import MONGO_DB, MONGO_URL, COLLECTIONS

"""
STEPS:

6. Add Films to Film.all_films
7. Add Actors to Actor.all_actors
8. For each film, get aggregates
9. Save to MongoDB
"""
# Queue of Film objects with only a mojo_id, mojo_title and mojo_year
raw_mojo_q = Queue()
# Queue of Film objects with an imdb_id that need to be scraped
film_todo_q = SetQueue()
# Queue of Actor objects with an imdb_id that need to be scraped
actor_todo_q = SetQueue()
# Queue of finished Film objects
film_output_q = SetQueue()
# Queue of finished Actor objects
actor_output_q = SetQueue()


def start_consumers():
    """
    Starts each consumer
    :return: array of running consumers
    """
    cs = [FindIMDbConsumer(raw_mojo_q, film_todo_q), ScrapeIMDbConsumer(film_todo_q, film_output_q, actor_todo_q),
                 ActorConsumer(actor_todo_q, actor_output_q)]
    for c in cs:
        c.start()
    return consumers


def drop_collections(db):
    """
    Drops existing collections
    :return: Nothing
    """
    print "Dropping current MongoDB collections..."
    cols = db.collection_names()
    for c in COLLECTIONS:
        if c in cols:
            print "Dropping {0}...".format(c)
            db.drop_collection(c)


def get_bom_movies(l, p):
    """
    Gets the title table from a box office mojo page
    :param l: the page letter
    :param p: the page number
    :return: a list of bs'd elements
    """
    url = "http://www.boxofficemojo.com/movies/alphabetical.htm?letter={0}&page={1}&p=.htm".format(l, p)
    res = requests.get(url)
    if res.status_code >= 400:
        return []
    soup = BeautifulSoup(res.content, "html.parser")
    tds = soup.find_all('td')
    titles_table = []
    for t in tds:
        if "Total Gross / Theaters" in t.text.strip():
            titles_table = t.parent.parent
            break
    return titles_table

if __name__ == '__main__':
    # Page letters
    letters = ['NUM', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
               'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    consumers = start_consumers()
    for letter in letters:
        page = 1
        table = get_bom_movies(letter, str(page))
        # While there's a table and there's at least one film in it
        while table and len(table) > 1:
            # Skipping the header row
            for td in table[1:]:
                try:
                    # If revenue is not applicable
                    if td[2].text.strip() == 'n/a':
                        continue
                    title = td[0].text.strip()
                    mojo_id = td[0].find('a')['href'].text.repleace('/movies/?id=')
                    mojo_year = str(datetime.strptime(td[6].text.strip(), "%d/%m/%Y").year)
                    raw_mojo_q.put(Film(mojo_id, title, mojo_year))
                except:
                    print "Failed while processing: {0}".format(str(td))
            # Get the next page of films
            page += 1
            table = get_bom_movies(letter, str(page))
    while not (raw_mojo_q.empty() and film_todo_q.empty() and actor_todo_q.empty()):
        print "raw: {0} films: {1} actors: {2}".format(raw_mojo_q.qsize(), film_todo_q.qsize(), actor_todo_q.qsize())
        sleep(5)

    # Add all finished films to the Film.dict
    while not film_output_q.empty():
        q_film = film_output_q.get()
        film_output_q.task_done()
        Film.all_films[q_film.id] = q_film

    # Add all finished actors to the Actor.dict
    while not actor_output_q.empty():
        a_film = actor_output_q.get()
        actor_output_q.task_done()
        Actor.all_actors[a_film.id] = a_film

    # Call aggregate on every film
    for f_id in Film.all_films:
        Film.all_films[f_id].set_aggregate_fields()

    print "Connecting to {0}...".format(MONGO_URL)
    client = pymongo.MongoClient(MONGO_DB)
    print "Linking to the following data: {0}...".format(MONGO_DB)
    db_conn = client[MONGO_DB]
    drop_collections(db_conn)

    # Save all of the films to MongoDb
    for f_id in Film.all_films:
        db_conn['films'].insert(Film.all_films[f_id].export())

    # Save all of the actors to MongoDb
    for a_id in Actor.all_actors:
        db_conn['actors'].insert(Actor.all_actors[a_id].export())
