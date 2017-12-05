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
from SaveActorConsumer import SaveActorConsumer
from SaveFilmConsumer import SaveFilmConsumer
from ScrapeIMDbConsumer import ScrapeIMDbConsumer
from SetQueue import SetQueue
from time import sleep
from config.GLOBALS import MONGO_DB, MONGO_URL, COLLECTIONS


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


def get_seen_db_films(db):
    """
    Gets all of the movies already seen in the database
    :param db: data base connection
    :return: set(imdb_id)
    """
    return set(db['films'].distinct("id", {}))


def get_scraped_mojo_ids(db):
    """
        Gets all of the mojo_ids of movies already processed that passed
        :param db: data base connection
        :return: set(mojo_id)
        """
    return set(db['films'].distinct("mojo_id", {}))


def get_seen_db_actors(db):
    """
        Gets all of the actors already seen in the database
        :param db: data base connection
        :return: set(imdb_id)
        """
    return set(db['actors'].distinct("id", {"DIRECTOR": False}))


def get_all_film_people(db):
    """
        Gets all of the actors and directors referenced by films
        :param db: data base connection
        :return: [{'id': str, 'DIRECTOR': boolean}]
        """
    films = db['films'].find({})
    unique_people = set()
    peoples = []
    for f in films:
        director = "director-{0}".format(f.get('director', ''))
        if director not in unique_people:
            peoples.append({'id': f.get('director', ''), 'DIRECTOR': True})
            unique_people.add(director)
        for a in f.get('actors', []):
            if a not in unique_people:
                peoples.append({'id': a, 'DIRECTOR': False})
                unique_people.add(a)
    return peoples


def get_seen_db_directors(db):
    """
        Gets all of the directors already seen in the database
        :param db: data base connection
        :return: set(director-imdb_id)
        """
    return set("director-{0}".format(imdb_id) for imdb_id in db['actors'].distinct("id", {"DIRECTOR": True}))


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
    movie_rows = []
    for a in soup.find_all('a'):
        if '/movies/?id=' in a.get('href', ''):
            movie_rows.append(a.parent.parent.parent)
    return [m for m in movie_rows if m.name == 'tr']


def main():
    print "Connecting to {0}...".format(MONGO_URL)
    client = pymongo.MongoClient(MONGO_URL)
    print "Linking to the following data: {0}...".format(MONGO_DB)
    db_conn = client[MONGO_DB]
    # Seen films
    seen_films = get_seen_db_films(db_conn)
    seen_mojos = get_scraped_mojo_ids(db_conn)
    seen_people = get_seen_db_actors(db_conn).union(get_seen_db_directors(db_conn))
    add_to_people_q = get_all_film_people(db_conn)
    # Queue of Film objects with only a mojo_id, mojo_title and mojo_year
    raw_mojo_q = SetQueue(starting_set=seen_mojos.copy())
    # Queue of Film objects with an imdb_id that need to be scraped
    film_todo_q = SetQueue(starting_set=seen_films.copy())
    # Queue of Films to be saved
    film_save_q = SetQueue(starting_set=seen_films.copy())
    # Queue of Actors to be saved
    actor_save_q = SetQueue(starting_set=seen_people.copy())
    # Queue of Actor objects with an imdb_id that need to be scraped
    actor_todo_q = SetQueue(starting_set=seen_people.copy())
    for p in add_to_people_q:
        if p['DIRECTOR']:
            actor_todo_q.put(Actor(p['id'], True), "director-{0}".format(p['id']))
        else:
            actor_todo_q.put(Actor(p['id'], False), p['id'])
    # Queue of finished Film objects
    film_output_q = SetQueue(starting_set=seen_films.copy())
    # Queue of finished Actor objects
    actor_output_q = SetQueue(starting_set=seen_people.copy())
    # Page letters
    letters = ['NUM', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
               'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    def start_consumers():
        """
        Starts each consumer
        :return: array of running consumers
        """
        cs = [FindIMDbConsumer(raw_mojo_q, film_todo_q), ScrapeIMDbConsumer(film_todo_q, film_save_q, actor_todo_q),
              ActorConsumer(actor_todo_q, actor_save_q), SaveActorConsumer(actor_save_q, actor_output_q),
              SaveFilmConsumer(film_save_q, film_output_q)]
        for c in cs:
            c.start()
        return cs

    consumers = start_consumers()
    for letter in letters:
        page = 1
        table = get_bom_movies(letter, str(page))
        while table:
            print "Starting scrape of {0}{1}...".format(letter, str(page))
            for tr in table:
                try:
                    tds = tr.find_all('td')
                    if len(tds) < 7:
                        continue
                    # If revenue is not applicable
                    if tds[2].text.strip() == 'n/a':
                        continue
                    title = tds[0].text.strip()
                    mojo_id = tds[0].find('a')['href'].replace('/movies/?id=', '').replace('.htm', '')
                    mojo_year = str(datetime.strptime(tds[6].text.strip(), "%m/%d/%Y").year)
                    raw_mojo_q.put(Film(mojo_id, title, mojo_year), mojo_id)
                except:
                    print "Failed while processing: {0}".format(str(tr))
            if letter == 'NUM':
                break
            # Get the next page of films
            page += 1
            table = get_bom_movies(letter, str(page))

    while not (raw_mojo_q.empty() and film_todo_q.empty() and actor_todo_q.empty() and actor_save_q.empty() and film_save_q.empty()):
        print "raw: {0} films: {1} actors: {2} save_films: {3} save_actors: {4}".format(
            raw_mojo_q.qsize(), film_todo_q.qsize(), actor_todo_q.qsize(), film_save_q.qsize(), actor_save_q.qsize())
        sleep(5)
