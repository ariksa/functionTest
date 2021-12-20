import logging

from manager import start as start_manager

PORT = 8000

logging.basicConfig(format='%(asctime)s - %(process)d - [%(levelname)s] %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')


def main():
    logging.info("The program is up!")
    start_manager(PORT)


if __name__ == '__main__':
    main()
