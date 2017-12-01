""" Consumes films scraped from BOM and tries to find IMDb page - add to next Queue to scrape Film details """
from QueueConsumer import QueueConsumer


class FindIMDbConsumer(QueueConsumer):

    def consume(self):
        """
        Finds the IMDb id for a film if possible
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        film_todo = self.input_q.get()
        self.input_q.task_done()
        # Sets imdb_id and returns the result ('' if not found)
        if film_todo.set_imdb_id():
            self.output_q.put(film_todo)
