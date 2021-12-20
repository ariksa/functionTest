import logging
from http import HTTPStatus
from multiprocessing import Process, Lock as MP_Lock
from threading import Thread

from flask import Flask, request
from waitress import serve

from lambda_code import lambda_function
from pipes import ProcessPipes
from stats import Stats

LAMBDA_GRACE_PERIOD_SEC = 5
FILE_LOCK = MP_Lock()
FILE_NAME = "lambda_output.txt"
PROCESS_PIPES = ProcessPipes()
PIPE_LIST = list()
STATS = Stats()
app = Flask("Manager")


def create_lambda(message):
    STATS.increase_active_instances()

    global PIPE_LIST
    if PIPE_LIST:
        process_pipes = PIPE_LIST.pop()
        reuse_lambda_once(message, process_pipes)
    else:
        process_pipes = spawn_process(message)

    reuse_lambda_loop(process_pipes)


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
        logging.debug("Reusing lambda once")
        process_pipes.message_write_pipe.send(message)
    except Exception as e:
        spawn_process(message)


def reuse_lambda_loop(process_pipes):
    logging.debug("Reusing lambda loop")

    global PIPE_LIST

    while True:
        # Get a message from the process
        if process_pipes.ack_read_pipe.poll(LAMBDA_GRACE_PERIOD_SEC * 2):
            message = process_pipes.ack_read_pipe.recv()
            if message == "Sleeping":
                # We can reuse the process while it sleeps
                STATS.decrease_active_instances()
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

    STATS.increase_invocation_count()

    return _handle_message(message)


@app.route("/statistics", methods=['GET'])
def get_statistics():
    logging.debug("GET statistics")

    data = {'active_instances': STATS.get_active_instance(),
            'total_invocation': STATS.get_invocation_count()}
    return data, HTTPStatus.OK
