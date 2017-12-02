""" Class to handle a Queue for multiprocessing and a set of seen items to avoid repeat processes """
from Queue import Queue
from multiprocessing import Lock


class SetQueue:
    def __init__(self):
        self.seen = set()
        self.seen_lock = Lock()
        self.queue = Queue()

    def is_seen(self, item, id_check=''):
        """
        Checks if a value is in the set
        :param item: an item
        :param id_check: Optional arg to use to check ID instead
        :return: Boolean
        """
        with self.seen_lock:
            if id_check:
                return id_check in self.seen
            return item in self.seen

    def mark_seen(self, item, id_check=''):
        """
        Marks an item as seen
        :param item: an item
        :param id_check: Optional arg to use to check ID instead
        :mutate seen: add an item to the set
        :return: Nothing
        """
        with self.seen_lock:
            if id_check:
                self.seen.add(id_check)
            else:
                self.seen.add(str(item))

    def put(self, item, id_check=''):
        """
        Puts an item in the Queue if it hasn't been seen
        :param item: an item
        :param id_check: Optional arg to use to check ID instead
        :mutate seen: adds an item
        :mutate queue: adds an item
        :exception: small chance some error will happen between is_seen and mark_seen
        :return: Nothing
        """
        if not self.is_seen(item, id_check):
            self.mark_seen(item, id_check)
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

    def empty(self):
        """
        Checks if the Queue is empty
        :return: Boolean
        """
        return self.queue.empty()

    def qsize(self):
        """
        Returns approx. length of queue
        :return: int
        """
        return self.queue.qsize()
