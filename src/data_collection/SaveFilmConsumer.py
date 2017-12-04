""" Takes Film from ScrapeIMDb, and adds them to mongodb """
import pymongo
from config.GLOBALS import MONGO_DB, MONGO_URL
from QueueConsumer import QueueConsumer


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
        self.collection.insert(film_todo.export())
        self.output_q.put(film_todo, film_todo.id)
