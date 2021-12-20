import logging
from time import sleep

INTERVAL_TO_SLEEP_SEC = 5


def lambda_function(message, file_lock, file_name):
    logging.debug(f"Got message {message}")
    # a.k.a. - working
    sleep(INTERVAL_TO_SLEEP_SEC)

    logging.debug(f"Writing message {message}")
    with file_lock:
        with open(file_name, 'a') as the_file:
            the_file.write(message + '\n')
