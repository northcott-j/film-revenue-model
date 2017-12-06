""" Takes an IMDb link and uses the model to predict revenue """
import sys
from data_collection.Film import Film
from data_collection.Actor import Actor
from data_collection.SetQueue import SetQueue

INTERCEPT = -8.989666e+06
COEFFICIENTS = {
    "length": 4.721269e+05,
    "mpaa": 7.246147e+06,
    "budget": 1.517801e-02,
    "weekday": -1.731469e+06,
    "avg_actor_film_appearances": -1.299648e+06,
    "max_actor_film_appearances": 1.387236e+06,
    "director_number_of_films": -1.530691e+06,
    "avg_actor_film_revenue": 2.008412e-01,
    "avg_director_film_revenue": 2.181338e-01,
    "max_director_film_revenue": 1.553259e-01,
    "director_age": -6.345087e+05,
    "avg_actor_film_stars": -4.284746e+06,
    "max_director_film_stars": -1.484372e+06,
    "max_actor_film_votes": 1.273199e+01,
    "avg_director_film_votes": 1.631983e+02,
    "max_actor_film_revenue": 6.754223e-02
}
MPAA_NUM = {
    "NC-17": 1,
    "R": 2,
    "PG-13": 3,
    "M/PG": 4,
    "PG": 5,
    "PASSED": 6,
    "G": 7
}


def main(imdb_link):
    actors = SetQueue()
    films = SetQueue()
    imdb_id = imdb_link.replace('/title/', '').split('/')[0].encode('utf-8')
    main_film = Film('NoId', 'NoTitle', '3000')
    main_film.set_non_aggregate_predict_fields(imdb_id)
    if main_film.FAILED:
        print "Can't predict the revenue for this film! Couldn't scrape a critical value"
        sys.exit()
    # Mark this film as seen but don't add to Queue and add to Film dict
    films.mark_seen(main_film, imdb_id)
    Film.all_films[imdb_id] = main_film
    # Add all of the actors and director to the actors queue
    for a in main_film.actors:
        actors.put(Actor(a, False), a)
    if main_film.director:
        actors.put(Actor(main_film.director, True), "director-{0}".format(main_film.director))
    # Scrape each actor
    while not actors.empty():
        a = actors.get()
        actors.task_done()
        a.set_non_aggregate_fields()
        if not a.FAILED:
            if a.DIRECTOR:
                Actor.all_actors["director-{0}".format(a.id)] = a
            else:
                Actor.all_actors[a.id] = a
        # Add their films to the Film queue
        for f in a.films:
            # Create a generic film object
            a_film = Film('NoId', 'NoTitle', '3000')
            # Force set the imdb id
            a_film.imdb_id = f
            films.put(Film, f)
    # Scrape each film
    while not films.empty():
        f = films.get()
        films.task_done()
        f.set_non_aggregate_predict_fields()

        
## TODO :
"""

This doesn't currently work because I do not have a way to map imdb titles to bom titles

"""