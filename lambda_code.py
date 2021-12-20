import logging
from time import sleep

INTERVAL_TO_SLEEP_SEC = 5


def lambda_function(message, file_lock, file_name, message_read_pipe, ack_write_pipe, grace_period_sec):
    logging.debug(f"Got message {message}")
    do_some_work(file_lock, file_name, message)
    grace_period(file_lock, file_name, message_read_pipe, ack_write_pipe, grace_period_sec)


def do_some_work(file_lock, file_name, message):
    sleep(INTERVAL_TO_SLEEP_SEC)
    logging.info(f"Writing message {message}")
    with file_lock:
        with open(file_name, 'a') as the_file:
            the_file.write(message + '\n')


def grace_period(file_lock, file_name, message_read_pipe, ack_write_pipe, grace_period_sec):
    logging.debug("Sleeping")
    ack_write_pipe.send("Sleeping")
    if message_read_pipe.poll(grace_period_sec):
        message = message_read_pipe.recv()
        logging.debug(f"Something is in the pipe! {message}")
        lambda_function(message, file_lock, file_name, message_read_pipe, ack_write_pipe, grace_period_sec)
    else:
        logging.debug("Harakiri")
        ack_write_pipe.send('Harakiri')
