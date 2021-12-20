from multiprocessing import Pipe


class ProcessPipes:

    def __init__(self):
        self.ack_read_pipe, self.ack_write_pipe = Pipe()
        self.message_read_pipe, self.message_write_pipe = Pipe()
