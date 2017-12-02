""" Takes Actor ID's output from ScrapeIMDbConsumer, scrapes 'em and adds to output Queue """
from QueueConsumer import QueueConsumer


class ActorConsumer(QueueConsumer):

    def consume(self):
        """
        Scrapes the non_aggregate fields for an actor
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """
        actor_todo = self.input_q.get()
        self.input_q.task_done()
        actor_todo.set_non_aggregate_fields()
        self.output_q.put(actor_todo, actor_todo.id)
