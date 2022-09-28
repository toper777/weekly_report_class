from loguru import logger


class MyLoggingException(Exception):
    def __init__(self, message):
        logger.exception(message)
