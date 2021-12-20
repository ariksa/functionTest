from threading import Lock


class Stats:
    def __init__(self):
        self.lock = Lock()
        self.active_instances = 0
        self.invocation_count = 0

    def get_active_instance(self):
        return self.active_instances

    def get_invocation_count(self):
        return self.invocation_count

    def increase_invocation_count(self):
        with self.lock:
            self.invocation_count += 1

    def increase_active_instances(self):
        with self.lock:
            self.active_instances += 1

    def decrease_active_instances(self):
        with self.lock:
            self.active_instances -= 1

