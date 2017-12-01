""" Class for a Film """
import datetime
import requests
import src.utils.print_colors as COLOR
from src.data_collection.Actor import Actor
from bs4 import BeautifulSoup
from urllib import urlencode


class Film:

    # Class wide instance of a dictionary used to point to all Film instances
    all_films = dict()

    def __init__(self, mojo_id, mojo_title, mojo_year):
        # If anything critical fails during the scrape process
        self.FAILED = False
        # The ID of the movie on Box Office Mojo                            !important
        self.mojo_id = mojo_id
        # The year from Box Office Mojo scrape to help filter IMDb results  !important
        self.mojo_year = str(mojo_year)
        # The Title found on Box Office Mojo                                !important
        self.mojo_title = mojo_title
        # The ID of the movie on IMDb (will be used as primary id)          !important
        self.id = ''
        # HTML of the IMDb page                                             !important
        self.imdb_page = None
        # HTML of the Box Office Mojo Page                                  !important
        self.mojo_page = None
        # The number of stars the film had outta 10
        self.stars = 0.0
        # The metascore outta 100
        self.metascore = 0
        # The number of user votes for the imdb star rating
        self.num_votes = 0
        # Film length in number of minutes
        self.length = 0
        # MPAA rating e.g. PG-13
        self.mpaa = None
        # Budget for the movie
        self.budget = 0
        # Datetime of film release in order to sort films                  !important
        self.release_date = None
        # Number of the month (1-12)
        self.month = 0
        # Day of the month (1-31)
        self.day = 0
        # The day of the week (1(Mon)-7)
        self.weekday = 0
        # This films revenue (used in model)                               !important
        self.revenue = 0
        # Director of the film (Actor id)
        self.director = ''
        # Actors that appeared in the film                                 !important
        self.actors = []

        ## AGGREGATE FIELDS (requires all Films and Actors to have been scraped)
        # Avg and Max number of films an Actor has been in
        self.avg_actor_film_appearances = 0
        self.max_actor_film_appearances = 0
        # Number of films that the director has directed
        self.director_number_of_films = 0
        # Avg and Max revenues for the array of Actors
        self.avg_actor_film_revenue = 0
        self.max_actor_film_revenue = 0
        # Avg and Max revenues for the director
        self.avg_director_film_revenue = 0
        self.max_director_film_revenue = 0
        # Avg age of the actors
        self.avg_actor_age = 0
        # Age of the director at the time of the film
        self.director_age = 0
        # Avg and Max star rating for the array of Actors
        self.avg_actor_film_stars = 0
        self.max_actor_film_stars = 0
        # Avg and Max star rating for the director
        self.avg_director_film_stars = 0
        self.max_director_film_stars = 0
        # Avg and Max metascore for the array of Actors
        self.avg_actor_film_metascore = 0
        self.max_actor_film_metascore = 0
        # Avg and Max metascore for the director
        self.avg_director_film_metascore = 0
        self.max_director_film_metascore = 0
        # Avg and Max number of film votes for the array of Actors
        self.avg_actor_film_votes = 0
        self.max_actor_film_votes = 0
        # Avg and Max number of film votes for the director
        self.avg_director_film_votes = 0
        self.max_director_film_votes = 0

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
            "mojo_page": {"value": BeautifulSoup("", "html.parser"), "important": True},
            "stars": {"value": 0.0, "important": False},
            "metascore": {"value": 0, "important": False},
            "num_votes": {"value": 0, "important": False},
            "length": {"value": 0, "important": False},
            "mpaa": {"value": 'None', "important": False},
            "budget": {"value": 0, "important": False},
            "release_date": {"value": datetime.datetime(3000, 1, 1, 0, 0), "important": True},
            "month": {"value": 0, "important": False},
            "day": {"value": 0, "important": False},
            "weekday": {"value": 0, "important": False},
            "revenue": {"value": 0, "important": True},
            "director": {"value": '', "important": False},
            "actors": {"value": [], "important": True},
            "avg_actor_film_appearances": {"value": 0, "important": False},
            "max_actor_film_appearances": {"value": 0, "important": False},
            "director_number_of_films": {"value": 0, "important": False},
            "avg_actor_film_revenue": {"value": 0, "important": False},
            "max_actor_film_revenue": {"value": 0, "important": False},
            "avg_director_film_revenue": {"value": 0, "important": False},
            "max_director_film_revenue": {"value": 0, "important": False},
            "avg_actor_age": {"value": 0, "important": False},
            "director_age": {"value": 0, "important": False},
            "avg_actor_film_stars": {"value": 0, "important": False},
            "max_actor_film_stars": {"value": 0, "important": False},
            "avg_director_film_stars": {"value": 0, "important": False},
            "max_director_film_stars": {"value": 0, "important": False},
            "avg_actor_film_metascore": {"value": 0, "important": False},
            "max_actor_film_metascore": {"value": 0, "important": False},
            "avg_director_film_metascore": {"value": 0, "important": False},
            "max_director_film_metascore": {"value": 0, "important": False},
            "avg_actor_film_votes": {"value": 0, "important": False},
            "max_actor_film_votes": {"value": 0, "important": False},
            "avg_director_film_votes": {"value": 0, "important": False},
            "max_director_film_votes": {"value": 0, "important": False}
        }
        print "{0}Film:{1} {2} field failed!!{3}".format(COLOR.FAIL, self.mojo_id, field, COLOR.ENDC)
        if fields[field]['important']:
            self.FAILED = True
        return fields[field]['value']

    def set_imdb_id(self):
        """
        Tries to search IMDb and find a matching film
        :mutate imdb_id: sets this field in the Film
        :return: String imdb_id
        """
        query = "http://www.imdb.com/find?ref_=nv_sr_fn&q={0}&s=all".format(self.mojo_title)
        encoded_query = urlencode(query)
        results = requests.get(encoded_query)
        # If the page cannot be found or IMDb broke
        if results.status_code >= 400:
            self.id = self.handle_error('id')
            return self.id
        html = BeautifulSoup(results.content, 'html.parser')
        sections = html.find_all('div', {'class': 'findSection'})
        for s in sections:
            header = s.find('h3', {'class': 'findSectionHeader'})
            if header and 'Titles' in header.text:
                title_table = s.find('table', {'class': 'findList'})
                title_sections = title_table.find_all('td', {'class': 'result_text'})
                for ts in title_sections:
                    if self.mojo_year in ts.text and ("Video Game" not in ts.text or "TV Episode" not in ts.text):
                        link = ts.find('a')['href']
                        # Format is normally href="/title/tt0086190/?ref_=fn_al_tt_1"
                        self.id = link.replace('/title/', '').split('/')[0]
                        return self.id
        self.id = self.handle_error('id')
        return self.id

    def set_non_aggregate_fields(self):
        """
        Calls each set function
        :mutate: Every field - see inner functions
        :return: Nothing
        """
        funcs = [self.set_imdb_page, self.set_mojo_page, self.set_stars, self.set_metascore, self.set_num_votes,
                 self.set_length, self.set_mpaa, self.set_budget, self.set_release_date, self.set_month, self.set_day,
                 self.set_weekday, self.set_director, self.set_actors, self.set_revenue]
        for f in funcs:
            print "{0}Calling {1} for {2}...{3}".format(COLOR.OKGREEN, str(f), self.mojo_title, COLOR.ENDC)
            f()

    def set_imdb_page(self):
        """
        Sets the self.imdb_page field
        :mutate imdb_page: updates this field
        :return: BS'd imdb page
        """
        page = requests.get("http://www.imdb.com/title/{0}/?ref_=fn_al_tt_1".format(self.id))
        if page.status_code >= 400:
            self.imdb_page = self.handle_error('imdb_page')
        else:
            self.imdb_page = BeautifulSoup(page.content, "html.parser")
        return self.imdb_page

    def set_mojo_page(self):
        """
        Sets the self.mojo_page field
        :mutate mojo_page: updates this field
        :return: BS'd mojo page
        """
        page = requests.get("http://www.boxofficemojo.com/movies/?id={0}.htm".format(self.mojo_id))
        if page.status_code >= 400:
            self.mojo_page = self.handle_error('mojo_page')
        else:
            self.mojo_page = BeautifulSoup(page.content, "html.parser")
        return self.mojo_page

    def set_stars(self):
        """
        Scrapes the star rating from the IMDb page
        :mutate stars: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: float the number of stars outta 10
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.stars = float(self.imdb_page.find('span', {'itemprop': 'ratingValue'}).text.strip())
        except:
            self.stars = self.handle_error('stars')
        return self.stars

    def set_metascore(self):
        """
        Scrapes the metascore rating from the IMDb page
        :mutate metascore: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: int a number from 0-100
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.metascore = int(self.imdb_page.find('div', {'class': 'metacriticScore'}).text.strip())
        except:
            self.metascore = self.handle_error('metascore')
        return self.metascore

    def set_num_votes(self):
        """
        Scrapes the number of user votes from the IMDb page
        :mutate num_votes: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: int
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.num_votes = int(self.imdb_page.find('span', {'itemprop': 'ratingCount'}).text.replace(',', '').strip())
        except:
            self.num_votes = self.handle_error('num_votes')
        return self.num_votes

    def set_length(self):
        """
        Scrapes the length of the film in minutes from the IMDb page
        :mutate length: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: int
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.length = int(self.imdb_page.find('time', {'itemprop': 'duration'}).text.replace('min', '').strip())
        except:
            self.length = self.handle_error('length')
        return self.length

    def set_mpaa(self):
        """
        Scrapes the mpaa rating from the IMDb page
        :mutate mpaa: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: string
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.mpaa = int(self.imdb_page.find('meta', {'itemprop': 'contentRating'})['content'].strip())
        except:
            self.mpaa = self.handle_error('mpaa')
        return self.mpaa

    def set_budget(self):
        """
        Scrapes the film approx budget from the IMDb page
        :mutate budget: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: int
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            txt_blocks = self.imdb_page.find_all('div', {'class': 'txt-block'})
            for tb in txt_blocks:
                h4 = tb.find('h4')
                if h4 and 'Budget' in h4.text:
                    budget = ''
                    for c in h4.text:
                        if c.isdigit():
                            budget += c
                    self.budget = int(budget)
        except:
            self.budget = self.handle_error('budget')
        return self.budget

    def set_release_date(self):
        """
        Scrapes the release date from the IMDb page
        :mutate release_date: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: datetime
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            self.release_date = datetime.datetime.strptime(self.imdb_page.find('meta', {'itemprop': 'datePublished'})['content'].strip(), "%Y-%m-%d")
        except:
            self.release_date = self.handle_error('release_date')
        return self.release_date

    def set_month(self):
        """
        gets the month that the film was released
        :mutate month: updates this field
        :mutate imdb_page: if the field isn't already filled
        :mutate release_date: if the field isn't already filled
        :return: int 0-12
        """
        if not self.imdb_page:
            self.set_imdb_page()
        if not self.release_date:
            self.set_release_date()
        try:
            self.month = self.release_date.month
        except:
            self.month = self.handle_error('month')
        return self.month

    def set_day(self):
        """
        gets the day of month that the film was released
        :mutate day: updates this field
        :mutate imdb_page: if the field isn't already filled
        :mutate release_date: if the field isn't already filled
        :return: int 0-31
        """
        if not self.imdb_page:
            self.set_imdb_page()
        if not self.release_date:
            self.set_release_date()
        try:
            self.day = self.release_date.day
        except:
            self.day = self.handle_error('day')
        return self.day

    def set_weekday(self):
        """
        gets the day of the week that the film was released
        :mutate weekday: updates this field
        :mutate imdb_page: if the field isn't already filled
        :mutate release_date: if the field isn't already filled
        :return: int 0-7
        """
        if not self.imdb_page:
            self.set_imdb_page()
        if not self.release_date:
            self.set_release_date()
        try:
            # Adding 1 to shift the scale from 0-6 to 1-7
            self.weekday = self.release_date.weekday() + 1
        except:
            self.weekday = self.handle_error('weekday')
        return self.weekday

    def set_director(self):
        """
        Scrapes the director id from the IMDb page
        :mutate director: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: string
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            director_span = self.imdb_page.find('span', {'itemprop': 'director'})
            if director_span:
                director_link = director_span.find('a')['href']
                self.director = director_link.replace('/name/', '').split('/')[0]
            else:
                self.director = self.handle_error('director')
        except:
            self.director = self.handle_error('director')
        return self.director

    def get_imdb_credits_page(self):
        """
        Gets the full actor page from IMDb
        :return: BS'd HTML
        """
        credits = requests.get('http://www.imdb.com/title/{0}/fullcredits'.format(self.id))
        if credits.status_code >= 400:
            return BeautifulSoup("", "html.parser")
        return BeautifulSoup(credits.content, "html.parser")

    def set_actors(self):
        """
        Scrapes the actor id's from the IMDb page
        :mutate actors: updates this field
        :mutate imdb_page: if the field isn't already filled
        :return: [string]
        """
        if not self.imdb_page:
            self.set_imdb_page()
        try:
            actor_ids = []
            credit_page = self.get_imdb_credits_page()
            actor_rows = credit_page.td('span', {'itemprop': 'actor'})
            if actor_rows:
                for ar in actor_rows:
                    link = ar.find('a')
                    if link:
                        actor_ids.append(link['href'].replace('/name/', '').split('/')[0])
                self.actors = actor_ids
            else:
                self.actors = self.handle_error('actors')
        except:
            self.actors = self.handle_error('actors')
        return self.actors

    def set_revenue(self):
        """
        Scrapes the film approx revenue from the Box Office Mojo page
        :mutate revenue: updates this field
        :mutate mojo_page: if the field isn't already filled
        :return: int
        """
        if not self.mojo_page:
            self.set_mojo_page()
        try:
            table_items = self.mojo_page.find_all('td')
            for ti in table_items:
                if '$' in ti.text and 'Worldwide:' in ti.previousSibling.text:
                    revenue = ''
                    for c in ti.text:
                        if c.isdigit():
                            revenue += c
                    self.revenue = int(revenue)
        except:
            self.revenue = self.handle_error('revenue')
        return self.revenue

    def get_release_date(self):
        """
        Gets the release date
        :mutate release_date: if the field doesn't exist
        :return: datetime
        """
        if not self.release_date:
            self.release_date = self.handle_error("release_date")
        return self.release_date

    def get_actors(self):
        """
        Gets the actors
        :mutate actors: if the field doesn't exist
        :return: [String]
        """
        if not self.actors:
            self.actors = self.handle_error("actors")
        return self.actors

    def get_revenue(self):
        """
        Gets the revenue
        :mutate revenue: if the field doesn't exist
        :return: int
        """
        if not self.revenue:
            self.revenue = self.handle_error("revenue")
        return self.revenue

    def get_stars(self):
        """
        Gets the stars
        :mutate stars: if the field doesn't exist
        :return: float
        """
        if not self.stars:
            self.stars = self.handle_error("stars")
        return self.stars

    def get_metascore(self):
        """
        Gets the metascore
        :mutate metascore: if the field doesn't exist
        :return: int
        """
        if not self.metascore:
            self.metascore = self.handle_error("metascore")
        return self.metascore

    def get_num_votes(self):
        """
        Gets the num_votes
        :mutate num_votes: if the field doesn't exist
        :return: int
        """
        if not self.num_votes:
            self.num_votes = self.handle_error("num_votes")
        return self.num_votes

    def export(self):
        """
        Exports the object as a dict
        :return: a dict of fields
        """
        fields = {
            "id": self.id,
            "FAILED": self.FAILED,
            "mojo_id": self.mojo_id,
            "mojo_title": self.mojo_title,
            "stars": self.stars,
            "metascore": self.metascore,
            "num_votes": self.num_votes,
            "length": self.length,
            "mpaa": self.mpaa,
            "budget": self.budget,
            "release_date": self.release_date,
            "month": self.month,
            "day": self.day,
            "weekday": self.weekday,
            "revenue": self.revenue,
            "director": self.director,
            "actors": self.actors,
            "avg_actor_film_appearances": self.avg_actor_film_appearances,
            "max_actor_film_appearances": self.max_actor_film_appearances,
            "director_number_of_films": self.director_number_of_films,
            "avg_actor_film_revenue": self.avg_actor_film_revenue,
            "max_actor_film_revenue": self.max_actor_film_revenue,
            "avg_director_film_revenue": self.avg_director_film_revenue,
            "max_director_film_revenue": self.max_director_film_revenue,
            "avg_actor_age": self.avg_actor_age,
            "director_age": self.director_age,
            "avg_actor_film_stars": self.avg_actor_film_stars,
            "max_actor_film_stars": self.max_actor_film_stars,
            "avg_director_film_stars": self.avg_director_film_stars,
            "max_director_film_stars": self.max_director_film_stars,
            "avg_actor_film_metascore": self.avg_actor_film_metascore,
            "max_actor_film_metascore": self.max_actor_film_metascore,
            "avg_director_film_metascore": self.avg_director_film_metascore,
            "max_director_film_metascore": self.max_director_film_metascore,
            "avg_actor_film_votes": self.avg_actor_film_votes,
            "max_actor_film_votes": self.max_actor_film_votes,
            "avg_director_film_votes": self.avg_director_film_votes,
            "max_director_film_votes": self.max_director_film_votes
        }
        return fields

    ## AGGREGATE METHODS
    @staticmethod
    def get_actor(actor_id):
        """
        Gets an Actor instance using an actor_id
        :param actor_id: string id of an actor
        :return: an Actor
        """
        return Actor.all_actors.get(actor_id, Actor('NonsenseActorId', False))

    def get_actor_stats(self, func):
        """
        Calculates sum, avg, max, and min stats of a field for the Actors
        :param func: a function that takes an Actor id and returns a numerical field
        :return: {sum: int, min: int, max: int, avg: int}
        """
        length = len(self.actors)
        sum_stat = 0
        max_stat = 0
        min_stat = 0
        for actor_id in self.actors:
            val = func(actor_id)
            # If the actor doesn't have a value, subtract from length and skip
            if not val:
                length -= 1
                continue
            sum_stat += val
            if val > max_stat:
                max_stat = val
            if val < min_stat:
                min_stat = val
        return {"sum": sum_stat, "max": max_stat, "min": min_stat, "avg": sum_stat / length}

    def set_aggregate_fields(self):
        """
        Calls each set function
        :mutate: Every field - see inner functions
        :return: Nothing
        """
        funcs = [self.set_actor_appearances, self.set_actor_revenue, self.set_actor_age, self.set_actor_stars,
                 self.set_actor_metascore, self.set_actor_votes, self.set_director_num_films, self.set_director_revenue,
                 self.set_director_age, self.set_director_stars, self.set_director_metascore, self.set_director_votes]
        for f in funcs:
            print "{0}Calling {1} for {2}...{3}".format(COLOR.OKGREEN, str(f), self.mojo_title, COLOR.ENDC)
            f()

    def set_actor_appearances(self):
        """
        Sets the avg and max number of actor appearances
        :mutate avg_actor_film_appearances: sets this field
        :mutate max_actor_film_appearances: sets this field
        :return: avg, max
        """
        stats = self.get_actor_stats(lambda a_id: self.get_actor(a_id).get_num_appearances_before(self.id))
        self.avg_actor_film_appearances = stats['avg']
        self.max_actor_film_appearances = stats['max']
        return self.avg_actor_film_appearances, self.max_actor_film_appearances

    def set_actor_revenue(self):
        """
        Sets the avg and max actor average film revenue
        :mutate avg_actor_film_revenue: sets this field
        :mutate max_actor_film_revenue: sets this field
        :return: avg, max
        """
        stats = self.get_actor_stats(lambda a_id: self.get_actor(a_id).get_avg_film_revenue_before(self.id))
        self.avg_actor_film_revenue = stats['avg']
        self.max_actor_film_revenue = stats['max']
        return self.avg_actor_film_revenue, self.max_actor_film_revenue

    def set_actor_age(self):
        """
        Sets the avg actor age
        :mutate avg_actor_age: sets this field
        :return: avg
        """
        stats = self.get_actor_stats(lambda a_id: self.get_actor(a_id).get_age_during(self.id))
        self.avg_actor_age = stats['avg']
        return self.avg_actor_age

    def set_actor_stars(self):
        """
        Sets the avg and max actor average star rating
        :mutate avg_actor_film_stars: sets this field
        :mutate max_actor_film_stars: sets this field
        :return: avg, max
        """
        stats = self.get_actor_stats(lambda a_id: self.get_actor(a_id).get_avg_film_stars_before(self.id))
        self.avg_actor_film_stars = stats['avg']
        self.max_actor_film_stars = stats['max']
        return self.avg_actor_film_stars, self.max_actor_film_stars

    def set_actor_metascore(self):
        """
        Sets the avg and max actor average metascore
        :mutate avg_actor_film_metascore: sets this field
        :mutate max_actor_film_metascore: sets this field
        :return: avg, max
        """
        stats = self.get_actor_stats(lambda a_id: self.get_actor(a_id).get_avg_film_metascore_before(self.id))
        self.avg_actor_film_metascore = stats['avg']
        self.max_actor_film_metascore = stats['max']
        return self.avg_actor_film_metascore, self.max_actor_film_metascore

    def set_actor_votes(self):
        """
        Sets the avg and max actor average votes per film
        :mutate avg_actor_film_votes: sets this field
        :mutate max_actor_film_votes: sets this field
        :return: avg, max
        """
        stats = self.get_actor_stats(lambda a_id: self.get_actor(a_id).get_avg_film_votes_before(self.id))
        self.avg_actor_film_votes = stats['avg']
        self.max_actor_film_votes = stats['max']
        return self.avg_actor_film_votes, self.max_actor_film_votes

    def set_director_num_films(self):
        """
        Sets the number of films that the director has directed
        :mutate director_number_of_films: sets this field
        :return: int
        """
        self.director_number_of_films = self.get_actor(self.director).get_num_appearances_before(self.id)
        return self.director_number_of_films

    def set_director_revenue(self):
        """
        Sets the avg and max director average film revenue
        :mutate avg_director_film_revenue: sets this field
        :mutate max_director_film_revenue: sets this field
        :return: avg, max
        """
        self.avg_director_film_revenue = self.get_actor(self.director).get_avg_film_revenue_before(self.id)
        self.max_director_film_revenue = self.get_actor(self.director).get_max_film_revenue_before(self.id)
        return self.avg_director_film_revenue, self.max_director_film_revenue

    def set_director_age(self):
        """
        Sets the avg director age
        :mutate avg_director_age: sets this field
        :return: avg
        """
        self.director_age = self.get_actor(self.director).get_age_during(self.id)
        return self.director_age

    def set_director_stars(self):
        """
        Sets the avg and max director average star rating
        :mutate avg_director_film_stars: sets this field
        :mutate max_director_film_stars: sets this field
        :return: avg, max
        """
        self.avg_director_film_stars = self.get_actor(self.director).get_avg_film_stars_before(self.id)
        self.max_director_film_stars = self.get_actor(self.director).get_max_film_stars_before(self.id)
        return self.avg_director_film_stars, self.max_director_film_stars

    def set_director_metascore(self):
        """
        Sets the avg and max director average metascore
        :mutate avg_director_film_metascore: sets this field
        :mutate max_director_film_metascore: sets this field
        :return: avg, max
        """
        self.avg_director_film_metascore = self.get_actor(self.director).get_avg_film_metascore_before(self.id)
        self.max_director_film_metascore = self.get_actor(self.director).get_max_film_metascore_before(self.id)
        return self.avg_director_film_metascore, self.max_director_film_metascore

    def set_director_votes(self):
        """
        Sets the avg and max director average votes per film
        :mutate avg_director_film_votes: sets this field
        :mutate max_director_film_votes: sets this field
        :return: avg, max
        """
        self.avg_director_film_votes = self.get_actor(self.director).get_avg_film_votes_before(self.id)
        self.max_director_film_votes = self.get_actor(self.director).get_max_film_votes_before(self.id)
        return self.avg_director_film_votes, self.max_director_film_votes
