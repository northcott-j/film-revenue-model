""" Consumes films scraped from BOM and tries to find IMDb page - add to next Queue to scrape Film details """
from QueueConsumer import QueueConsumer


class FindIMDbConsumer(QueueConsumer):

    def consume(self, input_q, output_q):
        """
        Finds the IMDb id for a film if possible
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        film_todo = input_q.get()
        input_q.task_done()
        # Sets imdb_id and returns the result ('' if not found)
        if film_todo.set_imdb_id():
            output_q.put(film_todo)
