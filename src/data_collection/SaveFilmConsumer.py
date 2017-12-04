""" Takes Film from ScrapeIMDb, and adds them to mongodb """
import pymongo
from config.GLOBALS import MONGO_DB, MONGO_URL
from QueueConsumer import QueueConsumer
from utils.print_colors import OKBLUE, ENDC


class SaveFilmConsumer(QueueConsumer):
    def __init__(self, ins, outs):
        QueueConsumer.__init__(self, ins, outs)
        client = pymongo.MongoClient(MONGO_URL)
        db_conn = client[MONGO_DB]
        self.collection = db_conn['films']

    def consume(self):
        """
        Saves a film to the database
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        film_todo = self.input_q.get()
        self.input_q.task_done()
        print "{0}Saving Film:{1} to the database...{2}".format(OKBLUE, film_todo.id, ENDC)
        self.collection.insert(film_todo.export())
        # Drop useless fields
        film_todo.purge()
        self.output_q.put(film_todo, film_todo.id)
