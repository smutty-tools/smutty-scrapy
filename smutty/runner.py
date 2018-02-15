import logging

from .exceptions import SmuttyException


def run(runnable_cls):
    try:
        runnable = runnable_cls()
        runnable.run()
    except SmuttyException as exception:
        logging.error("%s", exception)
    except Exception as exception:
        logging.critical("%s: %s", exception.__class__.__name__, exception)
        raise
