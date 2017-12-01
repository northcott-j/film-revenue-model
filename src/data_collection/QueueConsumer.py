""" Abstract class to handle taking an item, doing something, and adding it to an output Queue """
from abc import ABCMeta, abstractmethod
from threading import Thread


class QueueConsumer:
    __metaclass__ = ABCMeta

    def __init__(self, ins, outs):
        self.input_q = ins
        self.output_q = outs
        self.thread = None

    def start(self):
        """
        Starts a thread using the consume method and passes in and out queue
        :mutate thread: Starts and adds a thread
        :return: Nothing
        """
        self.thread = Thread(self.consume, args=(self, self.input_q, self.output_q,))
        self.thread.daemon = True
        self.thread.start()

    @abstractmethod
    def consume(self, input_q, output_q):
        """
        method to consume an item from the input and add it to the output
        :param input_q: queue of input items
        :param output_q: queue of output items
        :return: Nothing
        """