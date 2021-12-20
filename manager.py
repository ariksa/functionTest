import logging
from http import HTTPStatus
from multiprocessing import Process, Lock as MP_Lock
from threading import Thread, Lock as MT_Lock

from flask import Flask, request
from waitress import serve

from lambda_code import lambda_function

FILE_LOCK = MP_Lock()
FILE_NAME = "lambda_output.txt"
TOTAL_INVOCATION_LOCK = MT_Lock()
ACTIVE_INSTANCES = 0
TOTAL_INVOCATION_COUNT = 0
app = Flask("Manager")


def create_lambda(message):
    try:
        p = Process(target=lambda_function, args=(message, FILE_LOCK, FILE_NAME,))
        p.start()
        p.join()
    except Exception as e:
        logging.exception(e)


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
    with TOTAL_INVOCATION_LOCK:
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
