""" Class for an Actor (also Director) """
import datetime
import Film
import requests
from bs4 import BeautifulSoup
from utils.print_colors import OKGREEN, ENDC, FAIL


class Actor:
    # Class wide instance of a dictionary used to point to all Actor instances
    all_actors = dict()

    def __init__(self, imdb_id, director):
        # If there was a critical failure
        self.FAILED = False
        # If the person is a director - used to populate films field
        self.DIRECTOR = director
        # This is true if the films have been sorted
        self.FILMS_SORTED = False
        # ID found on IMDb                                                      !important
        self.id = imdb_id.encode('utf-8')
        # The Actor's name
        self.name = ''
        # The BS'd HTML of the IMDb page                                        !important
        self.imdb_page = None
        # The datetime birthday of the actor                                    !important
        self.birthday = None
        # List of IMDb film ID's that they've appeared in (needs to be sorted)  !important
        self.films = []

    def handle_error(self, field):
        """
        Knows default values for every field
        :param field: field to find the default value
        :mutate FAILED: if the field is important
        :return: default value for field
        """
        fields = {
            "id": {"value": '', "important": True},
            "imdb_page": {"value": BeautifulSoup("", "html.parser"), "important": True},
            "name": {"value": '', "important": False},
            "birthday": {"value": datetime.datetime(3000, 1, 1), "important": True},
            "films": {"value": [], "important": True}
        }
        print "{0}Actor:{1} {2} field failed!!{3}".format(FAIL, self.id, field, ENDC)
        if fields[field]['important']:
            self.FAILED = True
        return fields[field]['value']

    def set_non_aggregate_fields(self):
        """
        Calls each set function
        :mutate: Every field - see inner functions
        :return: Nothing
        """
        funcs = [self.set_imdb_page, self.set_name, self.set_birthday, self.set_films]
        for f in funcs:
            print "{0}Calling {1} for {2}...{3}".format(OKGREEN, str(f), self.id, ENDC)
            f()

    def set_imdb_page(self):
        """
        Sets the self.imdb_page field
        :mutate imdb_page: updates this field
        :return: BS'd imdb page
        """
        page = requests.get("http://www.imdb.com/name/{0}/?ref_=fn_al_tt_1".format(self.id))
        if page.status_code >= 400:
            self.imdb_page = self.handle_error('imdb_page')
        else:
            self.imdb_page = BeautifulSoup(page.content, "html.parser")
        return self.imdb_page

    def set_name(self):
        """
        Scrapes the actor name from the IMDb page
        :mutate name: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: string
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.name = self.imdb_page.find('span', {'itemprop': 'name'}).text.strip()
        except:
            self.name = self.handle_error('name')
        return self.name

    def set_birthday(self):
        """
        Scrapes the actor birthday from the IMDb page
        :mutate birthday: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: datetime
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            birthdate = self.imdb_page.find('time', {'itemprop': 'birthDate'})['datetime'].strip()
            # If only the year is available, set month and day to 1-1
            birthdate = birthdate.replace("-0-0", "-01-01")
            self.birthday = datetime.datetime.strptime(birthdate, "%Y-%m-%d")
        except:
            self.birthday = self.handle_error('birthday')
        return self.birthday

    def set_films(self):
        """
        Scrapes the film id's from the actor's IMDb page
        :mutate actors: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: [string]
        """
        self.FILMS_SORTED = False
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            film_ids = []
            if not self.DIRECTOR:
                films = self.imdb_page.find('div', {'id': 'filmo-head-actor'})
                replace_word = 'actor-'
                if not films:
                    films = self.imdb_page.find('div', {'id': 'filmo-head-actress'})
                    replace_word = 'actress-'
            else:
                films = self.imdb_page.find('div', {'id': 'filmo-head-director'})
                replace_word = 'director-'
            if films:
                film_section = films.findNext('div', {'class': 'filmo-category-section'})
                for film in film_section.find_all('div'):
                    div_id = film.get('id')
                    if div_id:
                        film_ids.append(div_id.replace(replace_word, "").strip())
                # should be in the order of newest to oldest so it needs to be reversed
                self.films = [f for f in reversed(film_ids)]
            else:
                self.films = self.handle_error('films')
        except:
            self.films = self.handle_error('films')
        return self.films

    def export(self):
        """
        Export the Actor as a dict
        :return: dict of fields
        """
        fields = {
            "id": self.id,
            "FAILED": self.FAILED,
            "DIRECTOR": self.DIRECTOR,
            "name": self.name,
            "birthday": self.birthday,
            "films": self.films
        }
        return fields

    def import_fields(self, fields):
        """
        Import an actor from dict
        :param fields: dictionary of values
        :mutate all fields: every field is updated
        :return: nothing
        """
        self.id = fields.get('id', self.handle_error('id'))
        self.FAILED = fields.get('FAILED', self.handle_error('FAILED'))
        self.DIRECTOR = fields.get('DIRECTOR', self.handle_error('DIRECTOR'))
        self.name = fields.get('name', self.handle_error('name'))
        self.birthday = fields.get('birthday', self.handle_error('birthday'))
        self.films = fields.get('films', self.handle_error('films'))

    def purge(self):
        """
        Removes imdb_page field to free up space
        :mutate imdb_page: sets to None
        :return: Nothing
        """
        self.imdb_page = None


    ## AGGREGATE METHODS
    @staticmethod
    def get_film(film_id):
        """
        Gets a Film instance using a film_id
        :param film_id: string id of a film
        :return: a Film
        """
        default = Film.Film('NonsenseFilmId', 'No Title', '3000')
        if Film.Film.all_films.get(film_id, None):
            if Film.Film.all_films[film_id].FAILED:
                return default
            else:
                return Film.Film.all_films[film_id]
        else:
            return default

    def sort_films(self):
        """
        Sorts the films from oldest to newest
        :mutate films: sorts the array
        :return: nothing
        """
        self.FILMS_SORTED = True
        self.films = sorted(self.films, key=lambda f_id: self.get_film(f_id).release_date)

    def get_film_average_before(self, func, stop):
        """
        Gets the average of a value for the films up to a certain film
        :param func: function that takes a film_id and gets a value
        :param stop: film ID to stop at
        :mutate films: sorts the array
        :return: average value (int)
        """
        if not self.FILMS_SORTED:
            self.sort_films()
        length = 0
        sum_stat = 0
        for f_id in self.films:
            if f_id == stop:
                break
            val = func(f_id)
            # If value isn't available, ignore the film and continue
            if not val:
                length -= 1
                continue
            length += 1
            sum_stat += val
        return sum_stat / length

    def get_film_max_before(self, func, stop):
        """
        Gets the max of a value for the films up to a certain film
        :param func: function that takes a film_id and gets a value
        :param stop: film id to stop at
        :mutate films: sorts the array
        :return: max value (int)
        """
        if not self.FILMS_SORTED:
            self.sort_films()
        max_stat = 0
        for f_id in self.films:
            if f_id == stop:
                break
            val = func(f_id)
            # If value isn't available, ignore the film and continue
            if not val:
                continue
            if val > max_stat:
                max_stat = val
        return max_stat

    def get_age_during(self, f_id):
        """
        Gets the approx. age of the actor during a film
        :param f_id: the id for the film
        :return: int
        """
        return self.get_film(f_id).get_release_date().year - self.birthday

    def get_num_appearances_before(self, f_id):
        """
        How many films an actor has been in before
        :param f_id:
        :return: the number of appearances before a movie
        """
        if not self.FILMS_SORTED:
            self.sort_films()
        try:
            return self.films.index(f_id)
        except:
            return None

    def get_avg_film_revenue_before(self, f_id):
        """
        Gets the average revenue before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_average_before(lambda fid: self.get_film(fid).get_revenue(), f_id)

    def get_max_film_revenue_before(self, f_id):
        """
        Gets the max revenue before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_max_before(lambda fid: self.get_film(fid).get_revenue(), f_id)

    def get_avg_film_stars_before(self, f_id):
        """
        Gets the average stars before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_average_before(lambda fid: self.get_film(fid).get_stars(), f_id)

    def get_max_film_stars_before(self, f_id):
        """
        Gets the max stars before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_max_before(lambda fid: self.get_film(fid).get_stars(), f_id)

    def get_avg_film_metascore_before(self, f_id):
        """
        Gets the average metascore before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_average_before(lambda fid: self.get_film(fid).get_metascore(), f_id)

    def get_max_film_metascore_before(self, f_id):
        """
        Gets the max metascore before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_max_before(lambda fid: self.get_film(fid).get_metascore(), f_id)

    def get_avg_film_votes_before(self, f_id):
        """
        Gets the average votes before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_average_before(lambda fid: self.get_film(fid).get_num_votes(), f_id)

    def get_max_film_votes_before(self, f_id):
        """
        Gets the max votes before a film
        :param f_id: the id for the film to stop at
        :mutate films: sorts if needed
        :return: int
        """
        return self.get_film_max_before(lambda fid: self.get_film(fid).get_num_votes(), f_id)
