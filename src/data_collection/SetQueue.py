""" Class to handle a Queue for multiprocessing and a set of seen items to avoid repeat processes """
from Queue import Queue
from multiprocessing import Lock


class SetQueue:
    def __init__(self):
        self.seen = set()
        self.seen_lock = Lock()
        self.queue = Queue()

    def is_seen(self, item):
        """
        Checks if a value is in the set
        :param item: an item
        :return: Boolean
        """
        with self.seen_lock:
            return item in self.seen

    def mark_seen(self, item):
        """
        Marks an item as seen
        :param item: an item
        :mutate seen: add an item to the set
        :return: Nothing
        """
        with self.seen_lock:
            self.seen.add(str(item))

    def put(self, item):
        """
        Puts an item in the Queue if it hasn't been seen
        :param item: an item
        :mutate seen: adds an item
        :mutate queue: adds an item
        :exception: small chance some error will happen between is_seen and mark_seen
        :return: Nothing
        """
        if not self.is_seen(item):
            self.mark_seen(item)
            return self.queue.put(item)

    def get(self):
        """
        Gets an item from the Queue
        :mutate queue: take an item
        :return: an item
        """
        return self.queue.get()

    def task_done(self):
        """
        Releases queue from lock after getting an item
        :return: Nothing
        """
        return self.queue.task_done()
