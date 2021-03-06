""" Scrapes IMDb info, add to a Queue of actors and a Queue of finished Films """
from QueueConsumer import QueueConsumer
from Actor import Actor


class ScrapeIMDbConsumer(QueueConsumer):
    # Have to have a queue to add all of the actors to
    def __init__(self, ins, outs, actors):
        QueueConsumer.__init__(self, ins, outs)
        self.actor_ins = actors

    def consume(self):
        """
        Finds the IMDb id for a film if possible
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        film_todo = self.input_q.get()
        self.input_q.task_done()
        film_todo.set_non_aggregate_fields()
        self.output_q.put(film_todo, film_todo.id)
        for a in film_todo.get_actors():
            self.actor_ins.put(Actor(a, False), a)
        self.actor_ins.put(Actor(film_todo.director, True), "director-{0}".format(film_todo.director))
