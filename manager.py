import logging
from http import HTTPStatus
from multiprocessing import Process, Pipe, Lock as MP_Lock
from threading import Thread, Lock as MT_Lock

from flask import Flask, request
from waitress import serve

from lambda_code import lambda_function

LAMBDA_GRACE_PERIOD_SEC = 50
FILE_LOCK = MP_Lock()
FILE_NAME = "lambda_output.txt"
TOTAL_INVOCATION_COUNT = 0
TOTAL_INVOCATION_COUNT_LOCK = MT_Lock()
ACTIVE_INSTANCES = 0
ACTIVE_INSTANCES_LOCK = MT_Lock()
PIPE_LIST = list()
app = Flask("Manager")


class PipeList:
    def __init__(self):
        pass


class ProcessPipes:
    def __init__(self):
        self.ack_read_pipe, self.ack_write_pipe = Pipe()
        self.message_read_pipe, self.message_write_pipe = Pipe()


def create_lambda(message):
    global ACTIVE_INSTANCES_LOCK
    global ACTIVE_INSTANCES
    with ACTIVE_INSTANCES_LOCK:
        ACTIVE_INSTANCES += 1

    global PIPE_LIST
    if PIPE_LIST:
        process_pipes = PIPE_LIST.pop()
        reuse_lambda_once(message, process_pipes)
    else:
        process_pipes = spawn_process(message)

    reuse_lambda_loop(process_pipes)

    with ACTIVE_INSTANCES_LOCK:
        ACTIVE_INSTANCES -= 1


def spawn_process(message):
    process_pipes = ProcessPipes()

    p = Process(target=lambda_function,
                args=(message, FILE_LOCK, FILE_NAME,
                      process_pipes.message_read_pipe,
                      process_pipes.ack_write_pipe,
                      LAMBDA_GRACE_PERIOD_SEC))
    p.start()
    return process_pipes


def reuse_lambda_once(message, process_pipes):
    try:
        process_pipes.message_write_pipe.send(message)
    except Exception as e:
        spawn_process(message)


def reuse_lambda_loop(process_pipes):
    logging.debug("Reusing lambda loop")

    global PIPE_LIST

    while True:
        if process_pipes.ack_read_pipe.poll(LAMBDA_GRACE_PERIOD_SEC * 2):
            message = process_pipes.ack_read_pipe.recv()
            if message == "Sleeping":
                PIPE_LIST.append(process_pipes)
            else:
                try:
                    logging.debug("The process has exited")
                    PIPE_LIST.remove(process_pipes)
                except ValueError:
                    pass

                return


def _handle_message(message):
    if not message:
        return "You have to provide a 'message' key", HTTPStatus.BAD_REQUEST

    t = Thread(target=create_lambda, args=(message,))
    t.start()

    return "Your request will be executed shortly...", HTTPStatus.ACCEPTED


def _truncate_file():
    with open(FILE_NAME, 'w'):
        pass


def start(port):
    logging.info(f"Starting manager on port {port}")
    _truncate_file()
    serve(app, host="0.0.0.0", port=port)


@app.route("/messages", methods=['POST'])
def post_message():
    logging.debug("POST messages")
    message = request.json.get('message')

    global TOTAL_INVOCATION_COUNT
    global TOTAL_INVOCATION_COUNT_LOCK

    with TOTAL_INVOCATION_COUNT_LOCK:
        TOTAL_INVOCATION_COUNT += 1

    return _handle_message(message)


@app.route("/statistics", methods=['GET'])
def get_statistics():
    logging.debug("GET statistics")

    global TOTAL_INVOCATION_COUNT
    global ACTIVE_INSTANCES
    data = {'active_instances': ACTIVE_INSTANCES,
            'total_invocation': TOTAL_INVOCATION_COUNT}
    return data, HTTPStatus.OK
