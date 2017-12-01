""" Scrapes IMDb info, add to a Queue of actors and a Queue of finished Films """
from QueueConsumer import QueueConsumer


class ScrapeIMDbConsumer(QueueConsumer):
    # Have to have a queue to add all of the actors to
    def __init__(self, ins, outs, actors):
        QueueConsumer.__init__(self, ins, outs)
        self.actor_ins = actors

    def consume(self, input_q, output_q):
        """
        Finds the IMDb id for a film if possible
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        film_todo = input_q.get()
        input_q.task_done()
        film_todo.set_non_aggregate_fields()
        for a in film_todo.get_actors():
            self.actor_ins.put(a)
