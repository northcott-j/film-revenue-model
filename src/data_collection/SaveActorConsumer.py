""" Takes Actor from ActorConsumer, and adds them to mongodb """
import pymongo
from config.GLOBALS import MONGO_DB, MONGO_URL
from QueueConsumer import QueueConsumer


class SaveActorConsumer(QueueConsumer):

    def __init__(self, ins, outs):
        QueueConsumer.__init__(self, ins, outs)
        client = pymongo.MongoClient(MONGO_URL)
        db_conn = client[MONGO_DB]
        self.collection = db_conn['actors']

    def consume(self):
        """
        Saves an actor to the database
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        actor_todo = self.input_q.get()
        self.input_q.task_done()
        self.collection.insert(actor_todo.export())
        # Drop useless fields
        actor_todo.purge()
        if actor_todo.DIRECTOR:
            self.output_q.put(actor_todo, "director-{0}".format(actor_todo.id))
        else:
            self.output_q.put(actor_todo, actor_todo.id)
